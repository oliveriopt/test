from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import yaml
from pathlib import Path
from datetime import datetime
import uuid

CONFIG_PATH = "include/config/qa_config.yaml"
BQ_CONN_ID = "google_cloud_default"
BQ_LOCATION = "us-central1"
PROJECT_ID = "rxo-dataeng-datalake-np"
REGION = "us-central1"
SERVICE_ACCOUNT = "ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com"
SUBNETWORK = "https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1"
TEMPLATE_GCS_PATH = "gs://rxo-dataeng-datalake-np-dataflow/templates/sql_to_parquet.json"


def read_config_task(**kwargs):
    config_path = Path(CONFIG_PATH)
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    tables = config["tables"]
    kwargs['ti'].xcom_push(key='tables_config', value=tables)


def prepare_query_tasks(**kwargs):
    ti = kwargs['ti']
    tables = ti.xcom_pull(task_ids='read_config', key='tables_config')
    bq_hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    task_inputs = []

    for table in tables:
        catalog = table["table_catalog"]
        schema = table["table_schema"]
        name = table["table_name"]

        q_sql = f"""
            SELECT query_text FROM `rxo-dataeng-datalake-np.dataops_admin.qa_query_plan`
            WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{name}'
            LIMIT 5
        """
        df_q = bq_hook.get_pandas_df(q_sql, location=BQ_LOCATION)
        if df_q.empty:
            raise ValueError(f"No queries found for {catalog}.{schema}.{name}")

        s_sql = f"""
            SELECT secret_id FROM `rxo-dataeng-datalake-np.dataops_admin.table_extraction_metadata`
            WHERE database_name = '{catalog}' AND schema_name = '{schema}' AND table_name = '{name}'
            LIMIT 1
        """
        df_s = bq_hook.get_pandas_df(s_sql, location=BQ_LOCATION)
        if df_s.empty:
            raise ValueError(f"No secret_id found for {catalog}.{schema}.{name}")

        secret_id = df_s["secret_id"].iloc[0]

        for query in df_q["query_text"].tolist():
            task_inputs.append({
                "table_catalog": catalog,
                "table_schema": schema,
                "table_name": name,
                "query": query,
                "secret_id": secret_id
            })

    ti.xcom_push(key="task_inputs", value=task_inputs)


def build_flex_body(**kwargs):
    return [
        {
            "task_id": f"run_flex_{i}",
            "body": {
                "launchParameter": {
                    "jobName": f"sqlserver-to-gcs-{conf['table_name'].lower()}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{i}",
                    "containerSpecGcsPath": TEMPLATE_GCS_PATH,
                    "environment": {
                        "serviceAccountEmail": SERVICE_ACCOUNT,
                        "subnetwork": SUBNETWORK,
                        "tempLocation": "gs://dataflow-staging-us-central1-387408803089/temp_files",
                        "stagingLocation": "gs://dataflow-staging-us-central1-387408803089/staging_area",
                        "ipConfiguration": "WORKER_IP_PRIVATE"
                    },
                    "parameters": {
                        "query": conf["query"],
                        "secret_id": conf["secret_id"],
                        "gcp_pr": PROJECT_ID,
                        "output_path": f"gs://rxo-dataeng-datalake-np-raw/qa/{conf['table_catalog']}/{conf['table_schema']}/{conf['table_name']}/{datetime.utcnow().strftime('%Y/%m/%d')}/data-{uuid.uuid4().hex[:8]}.parquet"
                    }
                }
            }
        }
        for i, conf in enumerate(kwargs['ti'].xcom_pull(task_ids='prepare_query_tasks', key='task_inputs'))
    ]


with DAG(
    dag_id="qa_flex_template_multiquery",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["qa", "bq", "dataflow"]
) as dag:

    start = EmptyOperator(task_id="start")

    read_config = PythonOperator(
        task_id="read_config",
        python_callable=read_config_task,
        provide_context=True,
    )

    prepare_query_tasks = PythonOperator(
        task_id="prepare_query_tasks",
        python_callable=prepare_query_tasks,
        provide_context=True,
    )

    def generate_dataflow_task(task_def):
        return DataflowStartFlexTemplateOperator(
            task_id=task_def["task_id"],
            project_id=PROJECT_ID,
            location=REGION,
            body=task_def["body"],
            gcp_conn_id="google_cloud_default",
        )

    start >> read_config >> prepare_query_tasks

    from airflow.operators.python import PythonOperator
    def trigger_all_flex_templates(**kwargs):
        from airflow.models.baseoperator import chain
        task_defs = build_flex_body(**kwargs)
        tasks = [generate_dataflow_task(defn) for defn in task_defs]
        chain(*tasks)

    trigger_tasks = PythonOperator(
        task_id="launch_flex_templates",
        python_callable=trigger_all_flex_templates,
        provide_context=True
    )

    prepare_query_tasks >> trigger_tasks
