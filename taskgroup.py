    task_groups = []
    for table in pipe_params["tables"]:
        with TaskGroup(group_id=f"{table['schema']}_{table['table']}_pipeline") as tg:
            task_id_prefix = f"{table['schema']}_{table['table']}"
            run_sql_to_gcs = DataflowStartFlexTemplateOperator(
                task_id=f"run_sql_to_gcs_{table['schema']}_{table['table']}_batched",
                project_id="rxo-dataeng-datalake-np",
                location="us-central1",
                body={
                    "launchParameter": {
                        "jobName": f"sql-gcs-{table['schema']}-{table['table']}-batched",
                        "containerSpecGcsPath": env_config["template_sql_to_gcs_batched"],
                        "environment": {
                            "serviceAccountEmail": env_config["dataflow_service_account"],
                            "subnetwork": env_config["subnetwork"],
                            "tempLocation": env_config["dataflow_temp_location"],
                            "stagingLocation": env_config["dataflow_staging_location"],
                            "ipConfiguration": env_config["ip_configuration"]
                        },
                        "parameters": {
                            "gcp_project": env_config["project_id"],
                            "schema": table['schema'],
                            "table" : table['table'],
                            "reload_flag": str(table["reload_flag"]).lower(),
                            "type_of_extraction" : table["type_of_extraction"],
                            "batch_size": str(table['batch_size']),
                            "output_gcs_path": table["output_gcs_path"],
                            "query": table["query"],
                            "secret_id": table["secret_id"],
                            "primary_key" : ",".join(table["primary_keys"]),
                            "num_workers" : str(table['num_workers']),
                            "max_num_workers" : str(table['max_num_workers']),
                            "autoscaling_algorithm" : str(table['autoscaling_algorithm']),
                            "machine_type" : str(table['machine_type']),
                            "chunk_size" : str(table['chunk_size'])
                            
                        }
                    }
                }
            )
