import logging
import json
import tds  # from python-tds
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import secretmanager
from google.cloud import bigquery

# --- Configuration ---
PROJECT_ID = "rxo-dataeng-datalake-np"
BQ_METADATA_TABLE = "rxo-dataeng-datalake-np.dataops_admin.table_extraction_metadata"

# --- Get metadata from BigQuery ---
def get_metadata_from_bq(table, database_name, schema_name):
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT
            secret_id,
            database_name,
            schema_name,
            table_name
        FROM `{BQ_METADATA_TABLE}`
        WHERE `table` = @table
          AND database_name = @database_name
          AND schema_name = @schema_name
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table", "STRING", table),
            bigquery.ScalarQueryParameter("database_name", "STRING", database_name),
            bigquery.ScalarQueryParameter("schema_name", "STRING", schema_name),
        ]
    )

    logging.info(f"Querying metadata for: table={table}, db={database_name}, schema={schema_name}")
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    row = next(iter(result), None)
    if row is None:
        raise ValueError(f"No metadata found for table '{table}' in database '{database_name}', schema '{schema_name}'")

    metadata = {
        "secret_id": row["secret_id"],
        "database_name": row["database_name"],
        "schema_name": row["schema_name"],
        "table_name": row["table_name"],
    }

    logging.info(f"Metadata retrieved: {metadata}")
    return metadata

# --- Get secret from Secret Manager ---
def get_sql_config(secret_id, project_id):
    logging.info(f"Accessing secret '{secret_id}' from project '{project_id}'")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    logging.info("Secret retrieved successfully.")
    return json.loads(secret_payload)

# --- Connect with python-tds and run SQL query ---
def connect_and_query_by_metadata(**kwargs):
    table = kwargs["params"]["table"]
    database_name = kwargs["params"]["database_name"]
    schema_name = kwargs["params"]["schema_name"]

    metadata = get_metadata_from_bq(table, database_name, schema_name)
    secret_id = metadata["secret_id"]
    table_name = metadata["table_name"]

    config = get_sql_config(secret_id, PROJECT_ID)

    full_table = f"[{schema_name}].[{table_name}]"
    query = f"SELECT TOP 1 * FROM {full_table}"

    logging.info(f"Connecting to SQL Server using tds...")
    conn = tds.connect(
        server=config["server"],
        database=database_name,
        user=config["username"],
        password=config["password"],
        port=1433,
        timeout=10
    )
    cursor = conn.cursor()
    logging.info(f"Running query: {query}")
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
    dag_id="query_sql_with_tds_driver",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_query = PythonOperator(
        task_id="query_sql_table",
        python_callable=connect_and_query_by_metadata,
        params={
            "table": "target_logical_table",       # logical name from metadata
            "database_name": "sales_dw",           # SQL Server DB
            "schema_name": "dbo",                  # SQL Server schema
        },
    )
