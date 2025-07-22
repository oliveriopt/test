
from google.cloud import secretmanager

def get_sql_config(secret_id, project_id):
    logging.info(f"Accessing secret '{secret_id}' from default project context")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    logging.info("Secret retrieved successfully.")
    return json.loads(secret_payload)


def build_connection_string(config):
    logging.info("Building SQL Server connection string...")
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},1433;"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )



def run_pipeline():
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project='rxo-dataeng-datalake-np',
        region='us-central1',
        temp_location='gs://rxo-dataeng-datalake-np-dataflow/temp',
        staging_location='gs://rxo-dataeng-datalake-np-dataflow/staging',
        save_main_session=True,
        service_account_email='ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com',
        subnetwork='https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1',
        use_public_ips=False,
        worker_harness_container_image='us-central1-docker.pkg.dev/rxo-dataeng-datalake-np/dataflow-flex-template/sql-parquet-to-gcs-batched-ap:latest'
    )
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    config = get_sql_config(custom_options.secret_id, custom_options.gcp_project)
    connection_string = build_connection_string(config)



import logging
import json
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import secretmanager

# Config
PROJECT_ID = "rxo-dataeng-datalake-np"
SECRET_ID = "sqlserver-connection"  # cambia esto a uno de los listados por list_secrets_task

# --- Secret Manager utilities ---
def get_sql_config(secret_id, project_id):
    logging.info(f"Accessing secret '{secret_id}' from project '{project_id}'")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    logging.info("Secret retrieved successfully.")
    return json.loads(secret_payload)

def build_connection_string(config):
    logging.info("Building SQL Server connection string...")
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},1433;"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )

# --- Task 1: List all secret_ids ---
def list_secrets(**kwargs):
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{PROJECT_ID}"
    secret_ids = []

    logging.info(f"Listing secrets for project: {PROJECT_ID}")
    for secret in client.list_secrets(request={"parent": parent}):
        name = secret.name.split("/")[-1]
        logging.info(f"Found secret: {name}")
        secret_ids.append(name)

    logging.info(f"Total secrets found: {len(secret_ids)}")
    return secret_ids

# --- Task 2: Use one specific secret to connect to SQL Server ---
def connect_to_sql_and_query(**kwargs):
    config = get_sql_config(SECRET_ID, PROJECT_ID)
    conn_str = build_connection_string(config)

    query = "SELECT TOP 1 * FROM your_table;"  # cambia esto a lo que necesites

    logging.info("Connecting to SQL Server...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(query)

    result = cursor.fetchone()
    logging.info(f"Query result: {result}")

    cursor.close()
    conn.close()

# --- DAG definition ---
default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="secret_manager_with_listing",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    list_secrets_task = PythonOperator(
        task_id="list_secrets_task",
        python_callable=list_secrets,
    )


https://teams.microsoft.com/l/meetup-join/19%253ameeting_Y2RkNjExZTgtN2U5Ni00NmIwLWI0OTUtMTY5NWE2YWM4YjAw%2540thread.v2/0?context=%257b%2522Tid%2522%253a%25227d0a4226-a0b0-47b3-b735-b2dcdd957579%2522%252c%2522Oid%2522%253a%25221383073a-1ad0-4802-9983-82c976781290%2522%257d
    query_sql_task = PythonOperator(
        task_id="query_sql_server",
        python_callable=connect_to_sql_and_query,
    )

    list_secrets_task >> query_sql_task
