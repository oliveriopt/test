
        task_id="log_success",
        python_callable=lambda **context: etl_logger.log_to_bigquery(
            context=context,
            status="success",
            message="DAG execution completed successfully"
        )
    )

log_start >> run_sqlserver_to_gcs >> check_sqlserver_job_status >> wait_for_file >> run_gcs_parquet_to_bq >> check_bq_job_status >> create_iceberg_table >> insert_iceberg_table >> log_success


 
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# Lista de combinaciones de base de datos y grupo de tablas
config_files = [
    "config_db1_group1.yaml",
    "config_db1_group2.yaml",
    "config_db2_group1.yaml",
    "config_db3_group1.yaml",
    # Añade hasta completar 50 bases x 2 grupos (100 configs aprox)
]

with DAG(
    dag_id="dag_master_parallel_launcher",
    start_date=datetime(2025, 6, 1),
    schedule_interval=None,
    catchup=False,
    tags=["launcher", "parallel"]
) as dag:

    # Crear una tarea TriggerDagRun por cada archivo de configuración
    for idx, config_file in enumerate(config_files):
        TriggerDagRunOperator(
            task_id=f"trigger_pipeline_{idx}",
            trigger_dag_id="dag_sqlserver_to_gcs_to_bq_to_iceberg_yaml_log",
            conf={"config_file": config_file},
            wait_for_completion=False  # Ejecuta de forma asíncrona
        )
