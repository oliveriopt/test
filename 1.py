o-gcs-batched.json"   --region "us-central1"   --project "rxo-dataeng-datalake-np"   --service-account-email "ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com"   --subnetwork "https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1"   --disable-public-ips   --staging-location "gs://rxo-dataeng-datalake-np-dataflow/staging"   --temp-location "gs://rxo-dataeng-datalake-np-dataflow/temp"   --parameters "gcp_project=rxo-dataeng-datalake-np,batch_size=2000,output_path=gs://us-central1-dataeng-dl-comp-4b3fa039-bucket/data/information_schema,secret_id=rxo-dataeng-datalake-np-brokerage-fo-mssql-xpomaster-uat-creds-connection-string,num_workers=30,max_num_workers=50,autoscaling_algorithm=THROUGHPUT_BASED,machine_type=e2-highmem-4,chunk_size=100000"^C



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowFlexTemplateOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import pandas as pd
import pyarrow.parquet as pq
import yaml
import tempfile
import os

# Configuración
BUCKET_NAME = 'us-central1-dataeng-dl-comp-4b3fa039-bucket'
INPUT_PARQUET = 'data/information_schema/structure.parquet'
OUTPUT_YAML = 'data/information_schema/sql_bronze_config.yaml'

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='bronze_flex_then_generate_yaml',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "dataflow", "yaml"]
) as dag:

    # 1. Ejecutar Flex Template de Dataflow
    run_flex_template = DataflowFlexTemplateOperator(
        task_id="run_o_gcs_batched_flex_template",
        project_id="rxo-dataeng-datalake-np",
        location="us-central1",
        body={
            "launchParameter": {
                "jobName": "o-gcs-batched-{{ ds_nodash }}",
                "containerSpecGcsPath": "gs://rxo-dataeng-datalake-np-dataflow/templates/o-gcs-batched.json",
                "environment": {
                    "serviceAccountEmail": "ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com",
                    "subnetwork": "https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1",
                    "tempLocation": "gs://rxo-dataeng-datalake-np-dataflow/temp",
                    "stagingLocation": "gs://rxo-dataeng-datalake-np-dataflow/staging",
                    "ipConfiguration": "WORKER_IP_PRIVATE"
                },
                "parameters": {
                    "gcp_project": "rxo-dataeng-datalake-np",
                    "batch_size": "2000",
                    "output_path": "gs://us-central1-dataeng-dl-comp-4b3fa039-bucket/data/information_schema",
                    "secret_id": "rxo-dataeng-datalake-np-brokerage-fo-mssql-xpomaster-uat-creds-connection-string",
                    "num_workers": "30",
                    "max_num_workers": "50",
                    "autoscaling_algorithm": "THROUGHPUT_BASED",
                    "machine_type": "e2-highmem-4",
                    "chunk_size": "100000"
                }
            }
        }
    )

    # 2. Leer Parquet generado por Dataflow y construir YAML
    def generate_yaml_from_parquet(ds, **kwargs):
        date_path = ds.replace("-", "/")  # e.g., 2025/07/10

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(INPUT_PARQUET)

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            blob.download_to_filename(tmp_file.name)
            df = pq.read_table(tmp_file.name).to_pandas()

        config = {"tables": []}
        grouped = df.groupby(["TABLE_SCHEMA", "TABLE_NAME"])
        for (schema, table), group in grouped:
            column_names = group["COLUMN_NAME"].unique().tolist()
            if not column_names:
                continue
            pk_col = column_names[0]
            table_config = {
                "name": table.lower(),
                "schema": schema,
                "primary_key": pk_col,
                "secret_id": "rxo-dataeng-datalake-np-brokerage-fo-mssql-xpomaster-uat-creds-connection-string",
                "output_path": f"gs://rxo-dataeng-datalake-np-raw/sql/brokerage-fo/XPOMaster/{schema}/{table}/{date_path}/"
            }
            config["tables"].append(table_config)

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.yaml') as tmp_yaml:
            yaml.dump(config, tmp_yaml, sort_keys=False)
            tmp_yaml.flush()
            output_blob = bucket.blob(OUTPUT_YAML)
            output_blob.upload_from_filename(tmp_yaml.name)

        os.unlink(tmp_file.name)
        os.unlink(tmp_yaml.name)

    generate_yaml = PythonOperator(
        task_id='generate_sql_bronze_config_yaml',
        python_callable=generate_yaml_from_parquet,
        provide_context=True,
        op_kwargs={"ds": "{{ ds }}"},
    )

    run_flex_template >> generate_yaml
