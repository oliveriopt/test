from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import yaml

# === Configuración global ===
default_args = {
    "owner": "airflow",
    "retries": 1,
}

dag_config_path = "data/information_schema/orders_order.yaml"

# === Paso 1: Cargar configuración desde YAML ===
def load_config(**kwargs):
    with open(dag_config_path, "r") as f:
        config = yaml.safe_load(f)
    required_keys = {"server_name", "schema_name", "table_name"}
    if not required_keys.issubset(config):
        raise ValueError(f"Missing keys in YAML config. Required: {required_keys}")
    
    # Guardamos en XCom
    kwargs["ti"].xcom_push(key="config", value=config)

# === Paso 2: Obtener la query desde BigQuery ===
def get_query(**kwargs):
    ti = kwargs["ti"]
    config = ti.xcom_pull(key="config", task_ids="load_config")

    server = config["server_name"]
    schema = config["schema_name"]
    table = config["table_name"]

    query_sql = """
        SELECT query_text
        FROM `rxo-dataeng-datalake-np.dataops_admin.qa_query_plan`
        WHERE server_name = @server
          AND schema_name = @schema
          AND table_name = @table
        LIMIT 1
    """
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    query_job = bq_hook.run_query(
        sql=query_sql,
        location="us-central1",
        params={"server": server, "schema": schema, "table": table},
        param_types={"server": "STRING", "schema": "STRING", "table": "STRING"},
    )
    df = bq_hook.get_pandas_df(query_job)
    if df.empty:
        raise ValueError(f"No QA query found for {server}.{schema}.{table}")

    query_text = df["query_text"].iloc[0]
    print(f"[QA QUERY] {query_text}")

    ti.xcom_push(key="query_text", value=query_text)

# === DAG ===
with DAG(
    dag_id="dag_qa_sqlserver_table",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["qa", "dataflow", "sqlserver"],
) as dag:

    load_config_task = PythonOperator(
        task_id="load_config",
        python_callable=load_config
    )

    get_query_task = PythonOperator(
        task_id="get_query_from_bq",
        python_callable=get_query
    )

    load_config_task >> get_query_task
