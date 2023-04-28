import yaml
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_endpoints.plugins.custom_operators.extract_operator import ExtractOperator
from airflow_endpoints.plugins.custom_operators.transform_operator import TransformOperator
from airflow_endpoints.plugins.custom_operators.load_operator import LoadOperator

# Load the ETL configuration from the etl_config.yaml file
with open("airflow_endpoints/config/dag_configs.yaml", "r") as f:
    config = yaml.safe_load(f)

dag_id = config["dag_id"]
schedule_interval = config["schedule_interval"]
default_args = config["default_args"]
default_args["retry_delay"] = timedelta(seconds=default_args["retry_delay"])

dag = DAG(
    dag_id,
    default_args=default_args,
    description="A generic ETL DAG",
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    catchup=False,
)

task_instances = {}

# Dynamically create tasks based on the configuration
for task_config in config["tasks"]:
    task_id = task_config["task_id"]
    task_type = task_config["task_type"]
    task_config.pop("task_id")
    task_config.pop("task_type")

    if task_type == "ExtractOperator":
        task = ExtractOperator(task_id=task_id, dag=dag, **task_config)
    elif task_type == "TransformOperator":
        task = TransformOperator(task_id=task_id, dag=dag, **task_config)
    elif task_type == "LoadOperator":
        task = LoadOperator(task_id=task_id, dag=dag, **task_config)
    else:
        raise ValueError(f"Unsupported task type: {task_type}")

    task_instances[task_id] = task

# Set up task dependencies
for dependency in config["dependencies"]:
    upstream_task = task_instances[dependency["upstream_task"]]
    downstream_task = task_instances[dependency["downstream_task"]]
    upstream_task >> downstream_task
