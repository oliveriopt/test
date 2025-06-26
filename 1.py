from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from include.utils import ConfigLoader, ETLLogger, DataflowLogger

 
# Load configurations
config_loader = ConfigLoader()
sql_params, bq_params, iceberg_params, env_config = config_loader.load_configurations()

# === Instantiate Loggers ===
etl_logger = ETLLogger(
    project_id=env_config["project_id"],
    dataset=env_config["dataset"],
    table=env_config["table"]
)

dataflow_logger = DataflowLogger(project_id=env_config["project_id"])

with DAG(
    dag_id="dag_sqlserver_to_gcs_to_bq_to_iceberg_yaml_log",
    start_date=days_ago(1),
    schedule_interval=None,  # Change to "*/30 * * * *" if needed
    catchup=False,
    tags=["dataflow", "etl", "yaml", "unified"]
) as dag:

    #Task 0: Make logger accessible from tasks
    dag.etl_logger = etl_logger

    log_start = PythonOperator(
        task_id="log_start",
        python_callable=lambda **context: etl_logger.log_to_bigquery(
            context=context,
            status="started",
            message="DAG execution started"
        )
    )

    # Task 1.a: SQL Server to GCS (Parquet)
    run_sqlserver_to_gcs = DataflowStartFlexTemplateOperator(
        task_id="run_sqlserver_to_gcs",
        project_id=env_config["project_id"],
        location=env_config["region"],
        body={
            "launchParameter": {
                "jobName": "sqlserver-to-gcs-yaml",
                "containerSpecGcsPath": env_config["template_sql_to_gcs"],
                "environment": {
                    "serviceAccountEmail": env_config["service_account"],
                    "subnetwork": env_config["subnetwork"],
                    "tempLocation": env_config["temp_location"],
                    "stagingLocation": env_config["staging_location"],
                    "ipConfiguration": "WORKER_IP_PRIVATE"
                },
                "parameters": {
                    "query": sql_params["query"],
                    "output_path": sql_params["output_path"],
                    "gcp_pr": env_config["project_id"],
                    "secret_id": sql_params["secret_id"]
                }
            }
        }
    )
    # Task 1.b: check status
    check_sqlserver_job_status = PythonOperator(
        task_id="check_sqlserver_job_status",
        python_callable=lambda **context: dataflow_logger.log_dataflow_status(
            context=context,
            task_ref="run_sqlserver_to_gcs",
            region=env_config["region"]
        )
    )

    # Task 2: Wait for Parquet file in GCS
    bucket = sql_params["output_path"].replace("gs://", "").split("/")[0]
    obj = "/".join(sql_params["output_path"].replace("gs://", "").split("/")[1:])

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_input_file",
        bucket=bucket,
        object=obj
    )

    # Task 3.a: GCS to BigQuery
    run_gcs_parquet_to_bq = DataflowStartFlexTemplateOperator(
        task_id="run_gcs_parquet_to_bq",
        project_id=env_config["project_id"],
        location=env_config["region"],
        body={
            "launchParameter": {
                "jobName": "gcs-parquet-to-bq-yaml",
                "containerSpecGcsPath": env_config["template_gcs_to_bq"],
                "environment": {
                    "serviceAccountEmail": env_config["service_account"],
                    "subnetwork": env_config["subnetwork"],
                    "tempLocation": env_config["temp_location"],
                    "stagingLocation": env_config["staging_location"],
                    "ipConfiguration": "WORKER_IP_PRIVATE"
                },
                "parameters": {
                    "input_file": bq_params["input_file"],
                    "output_table": bq_params["output_table"],
                    "write_disposition": bq_params["write_disposition"]
                }
            }
        }
    )

    # Task 3.b: check status
    check_bq_job_status = PythonOperator(
        task_id="check_bq_job_status",
        python_callable=lambda **context: dataflow_logger.log_dataflow_status(
            context=context,
            task_ref="run_gcs_parquet_to_bq",
            region=env_config["region"]
        )
    )

    # Step 4.a: Create bq table to iceberg format
    create_iceberg_table = BigQueryInsertJobOperator(
        task_id='create_iceberg_table',
        configuration={
            'query': {
                'query': iceberg_params["query_create"],
                'useLegacySql': False

            }
        }
    )

    # Task 4.b: check status (TO DO CHECK FOR BQ INSERT JOB OPERATOR)


    # Step 5.a: Insert data to iceberg format
    insert_iceberg_table = BigQueryInsertJobOperator(
        task_id='insert_iceberg_table',
        configuration={
            'query': {
                'query': iceberg_params["query_insert"],
                'useLegacySql': False

            }
        }
    )

    # Task 5.b: check status (TO DO CHECK FOR BQ INSERT JOB OPERATOR)

    # Task 6: Log success
    log_success = PythonOperator(
        task_id="log_success",
        python_callable=lambda **context: etl_logger.log_to_bigquery(
            context=context,
            status="success",
            message="DAG execution completed successfully"
        )
    )

log_start >> run_sqlserver_to_gcs >> check_sqlserver_job_status >> wait_for_file >> run_gcs_parquet_to_bq >> check_bq_job_status >> create_iceberg_table >> insert_iceberg_table >> log_success


 
