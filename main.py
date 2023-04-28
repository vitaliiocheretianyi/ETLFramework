# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

"""
This is my folder structure:

etl_framework/
├── airflow_endpoints/
│   ├── config/
│   │   ├── airflow.cfg
│   │   ├── dag_configs.yaml
│   └── dags/
│       └── generic_etl_dag.py
│   ├──  plugins/
│   │   ├── custom_hooks/
│   │   │   ├── api_hook.py
│   │   │   ├──database_hook.py
│   │   ├── custom_operators/
│   │   │   ├── extract_operator.py
│   │   │   ├── load_operator.py
│   │   │   ├──  transform_operator.py
├── data/
│   ├── dynamic_models.py
├── spark_jobs/
│   ├── reusable_spark_job.py
│   ├── spark_configs.yaml
├── utils/
│   ├── helpers.py

State of the app:
airflow.cfg - currently empty
dag_configs.yaml - currently empty
UML Diagrams:+------------------------------------+      +------------------------------------+
|           ExtractOperator          |      |          TransformOperator         |
+------------------------------------+      +------------------------------------+
| - api_hook: str                    |      | - input_file: str                  |
| - endpoint: str                    |      | - output_file: str                 |
| - output_file: str                 |      | - spark_job: str                   |
|                                    |      | - spark_config: str                |
| + execute(context: dict)           |      |                                    |
+------------------------------------+      | + execute(context: dict)           |
                                            +------------------------------------+
+------------------------------------+
|             LoadOperator           |
+------------------------------------+
| - input_data: str                  |
| - db_conn_id: str                  |
| - destination_table: str           |
| - model_builder: DynamicModel      |
|                                    |
| + execute(context: dict)           |
+------------------------------------+

+------------------------------------+
|          DynamicModel              |
+------------------------------------+
|                                    |
| + create_table_model(df: DataFrame,|
|    table_name: str) -> Base        |
+------------------------------------+

+------------------------------------+
|         APIExtractorHook           |
+------------------------------------+
|                                    |
| + __init__(http_conn_id: str,      |
|   *args, **kwargs)                 |
| + fetch_data(endpoint: str,        |
|   params=None) -> dict             |
+------------------------------------+

+------------------------------------+
|           DatabaseHook             |
+------------------------------------+
| - db_conn_id: str                  |
|                                    |
| + __init__(db_conn_id: str)        |
| + get_conn() -> Engine             |
| + get_session() -> contextmanager  |
+------------------------------------+

generic_etl_dag.py does:

This flowchart represents the high-level logic of the script:

Read the etl_config.yaml file to obtain the configuration for the ETL process.
Create an Airflow DAG using the extracted configuration.
Create tasks based on the configuration, which can be ExtractOperator, TransformOperator, or LoadOperator tasks.
Set up task dependencies as defined in the configuration file, establishing the sequence of tasks execution within the DAG.

UML flow chart for generic_etl_dag:
+-------------------------------+
| Read etl_config.yaml          |
+-------------------------------+
               |
               v
+-------------------------------+
| Create DAG                    |
+-------------------------------+
               |
               v
+-------------------------------+
| Create tasks from config      |
+-------------------------------+
               |
               v
+-------------------------------+
| Set up task dependencies      |
+-------------------------------+

reusable_spark_job.py:
+------------------------------------+
|         PySpark Transform          |
+------------------------------------+
|                                    |
| + load_config(config_file: str)    |
|   -> dict                          |
|                                    |
| + transform_data(spark: SparkSession,|
|   input_data: str, output_data: str,|
|   config_file: str)                |
+------------------------------------+

helpers.py:
+------------------------------------+
|         Column Type Infer          |
+------------------------------------+
|                                    |
| + infer_column_type(value)         |
|   -> Type[Union[Integer, String,   |
|      DateTime, Float]]             |
+------------------------------------+



"""