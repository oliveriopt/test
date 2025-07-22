rxo-dataeng-datalake-np.dataops_admin.table_extraction_metadata
import logging
import json
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import secretmanager
from google.cloud import bigquery

# --- Configuración general ---
PROJECT_ID = "rxo-dataeng-datalake-np"
BQ_METADATA_TABLE = "rxo-dataeng-datalake-np.dataops_admin.table_extraction_metadata"

# --- Función para obtener metadatos desde BigQuery ---
def get_metadata_from_bq(table_name, database_name, schema_name):
    """
    Recupera desde BigQuery la metadata asociada a una combinación específica de:
    - table (nombre lógico de la tabla)
    - database_name (base de datos SQL Server)
    - schema_name (esquema SQL Server)
    Retorna:
    - secret_id
    - database_name
    - schema_name
    - source_table (nombre real en SQL Server)
    """
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT
            secret_id,
            database_name,
            schema_name,
            source_table
        FROM `{BQ_METADATA_TABLE}`
        WHERE table = @table_name
          AND database_name = @database_name
          AND schema_name = @schema_name
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            bigquery.ScalarQueryParameter("database_name", "STRING", database_name),
            bigquery.ScalarQueryParameter("schema_name", "STRING", schema_name),
        ]
    )

    logging.info(f"Querying metadata for: table={table_name}, db={database_name}, schema={schema_name}")
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    row = next(iter(result), None)
    if row is None:
        raise ValueError(f"No metadata found for table '{table_name}' in database '{database_name}', schema '{schema_name}'")

    metadata = {
        "secret_id": row["secret_id"],
        "database_name": row["database_name"],
        "schema_name": row["schema_name"],
        "source_table": row["source_table"],
    }

    logging.info(f"Metadata retrieved: {metadata}")
    return metadata

# --- Función para recuperar el secreto desde Secret Manager ---
def get_sql_config(secret_id, project_id):
    logging.info(f"Accessing secret '{secret_id}' from project '{project_id}'")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    logging.info("Secret retrieved successfully.")
    return json.loads(secret_payload)

# --- Construcción del connection string ---
def build_connection_string(config, database_name):
    logging.info("Building SQL Server connection string...")
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},1433;"
        f"DATABASE={database_name};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )

# --- Tarea principal ---
def connect_and_query_by_metadata(**kwargs):
    table_name = kwargs["params"]["table"]
    database_name = kwargs["params"]["database_name"]
    schema_name = kwargs["params"]["schema_name"]

    metadata = get_metadata_from_bq(table_name, database_name, schema_name)
    secret_id = metadata["secret_id"]
    source_table = metadata["source_table"]

    config = get_sql_config(secret_id, PROJECT_ID)
    conn_str = build_connection_string(config, database_name)

    full_table = f"[{schema_name}].[{source_table}]"
    query = f"SELECT TOP 1 * FROM {full_table}"

    logging.info(f"Running query: {query}")
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
    dag_id="query_sql_by_table_and_metadata",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_query = PythonOperator(
        task_id="connect_and_query_sql",
        python_callable=connect_and_query_by_metadata,
        params={
            "table": "target_table_name",             # nombre lógico en tabla BQ
            "database_name": "sales_dw",              # base de datos SQL Server
            "schema_name": "dbo",                     # esquema SQL Server
        },
    )


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
