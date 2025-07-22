ecute_task\n    result = _execute_callable(context=context, **execute_callable_kwargs)\n             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 734, in _execute_callable\n    return ExecutionCallableRunner(\n           ^^^^^^^^^^^^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/airflow/utils/operator_helpers.py", line 252, in run\n    return self.func(*args, **kwargs)\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/airflow/models/baseoperator.py", line 415, in wrapper\n    return func(self, *args, **kwargs)\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/airflow/operators/python.py", line 238, in execute\n    return_value = self.execute_callable()\n                   ^^^^^^^^^^^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/airflow/operators/python.py", line 256, in execute_callable\n    return runner.run(*self.op_args, **self.op_kwargs)\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/airflow/utils/operator_helpers.py", line 252, in run\n    return self.func(*args, **kwargs)\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File "/home/airflow/gcs/dags/dag_qa.py", line 97, in connect_and_query_by_metadata\n    metadata = get_metadata_from_bq(table_name, database_name, schema_name)\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File "/home/airflow/gcs/dags/dag_qa.py", line 52, in get_metadata_from_bq\n    result = query_job.result()\n             ^^^^^^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/google/cloud/bigquery/job/query.py", line 1681, in result\n    while not is_job_done():\n              ^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/retry/retry_unary.py", line 293, in retry_wrapped_func\n    return retry_target(\n           ^^^^^^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/retry/retry_unary.py", line 153, in retry_target\n    _retry_error_helper(\n  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/retry/retry_base.py", line 212, in _retry_error_helper\n    raise final_exc from source_exc\n  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/retry/retry_unary.py", line 144, in retry_target\n    result = target()\n             ^^^^^^^^\n  File "/opt/python3.11/lib/python3.11/site-packages/google/cloud/bigquery/job/query.py", line 1630, in is_job_done\n    raise job_failed_exception\ngoogle.api_core.exceptions.BadRequest: 400 Unrecognized name: source_table at [6:13]; reason: invalidQuery, location: query, message: Unrecognized name: source_table at [6:13]\n\nLocation: us-central1\nJob ID: 0d840433-fd9f-4847-a24f-46a448caaef4\n 


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
        f"DATABASE={database_name};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )

# --- Main task ---
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

    logging.info(f"Executing query: {query}")
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
    dag_id="query_sql_by_metadata_keys",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_query = PythonOperator(
        task_id="query_sql_table",
        python_callable=connect_and_query_by_metadata,
        params={
            "table": "target_table_name",         # logical name in metadata
            "database_name": "sales_dw",          # SQL Server database
            "schema_name": "dbo",                 # SQL Server schema
        },
    )
