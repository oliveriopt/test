import logging
import json
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import secretmanager
from google.cloud import bigquery

# --- Global configuration ---
PROJECT_ID = "rxo-dataeng-datalake-np"
BQ_METADATA_TABLE = "rxo-dataeng-datalake-np.dataops_admin.table_extraction_metadata"

# --- Helper: Fetch metadata from BigQuery ---
def get_metadata_from_bq(table_name, database_name, schema_name):
    """
    Queries metadata for a given combination of:
    - table (logical table name)
    - database_name (SQL Server database)
    - schema_name (SQL Server schema)

    Returns:
    - secret_id
    - database_name
    - schema_name
    - source_table (actual SQL Server table name)
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

# --- Helper: Access Secret Manager ---
def get_sql_config(secret_id, project_id):
    logging.info(f"Accessing secret '{secret_id}' from project '{project_id}'")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    logging.info("Secret retrieved successfully.")
    return json.loads(secret_payload)

# --- Helper: Build connection string ---
def build_connection_string(config, database_name):
    logging.info("Building SQL Server connection string...")
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},1433;"
        f"

