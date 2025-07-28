from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.python import PythonSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

# Configuración
PROJECT_ID = "rxo-dataeng-datalake-np"
REGION = "us-central1"
SERVICE_ACCOUNT = "ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com"
SUBNETWORK = "https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1"
TEMPLATE_GCS_PATH = "gs://rxo-dataeng-datalake-np-dataflow/templates/sql_to_parquet.json"

# DAG principal
with DAG(
    dag_id="dag_sqlserver_to_gcs",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["dataflow", "gcs", "sqlserver"]
) as dag:

    run_flex_template = DataflowStartFlexTemplateOperator(
        task_id="run_sqlserver_to_gcs",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
            "jobName": "sqlserver-to-gcs-cron",
            "containerSpecGcsPath": TEMPLATE_GCS_PATH,
            "environment": {
            "serviceAccountEmail": SERVICE_ACCOUNT,
            "subnetwork": SUBNETWORK,
            "tempLocation": "gs://dataflow-staging-us-central1-387408803089/temp_files",
            "stagingLocation": "gs://dataflow-staging-us-central1-387408803089/staging_area",
            "ipConfiguration": "WORKER_IP_PRIVATE"
            },
        "parameters": {
            "query": "SELECT TOP 100 * FROM [orders].[Order]",
            "output_path": "gs://rxo-dataeng-datalake-np-raw/test-sql/orders.snappy.parquet",
            "gcp_pr": "rxo-dataeng-datalake-np",
            "secret_id":"brokerage-fo-sql-xpomaster-qa"
        }
   }
  }
 )

run_flex_template





