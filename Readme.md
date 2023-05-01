# ETL Framework

This repository contains an ETL framework built with Apache Airflow, PySpark, and SQLAlchemy. It allows you to configure and schedule data extraction, transformation, and loading tasks.

## How to Configure an ETL Job for Alpha Vantage Quarterly Financial Data

1. Obtain an API key from Alpha Vantage by registering at https://www.alphavantage.co/support/#api-key.
2. Configure an Airflow Connection to store the API key. Go to the Airflow UI, then **Admin** > **Connections** > **Create**. Set the following fields:
   - Conn Id: `alphavantage_api_key`
   - Conn Type: `HTTP`
   - Password: `<your_api_key>`
3. Update `dag_configs.yaml` to define the ETL tasks for extracting, transforming, and loading quarterly financial data from Alpha Vantage. An example configuration is provided below:

```yaml
dag_id: alphavantage_quarterly_financial_data_etl
schedule_interval: '@daily'
default_args:
  owner: 'etl_user'
  retries: 3
  retry_delay: 300  # 5 minutes in seconds
tasks:
  - task_id: extract_quarterly_financial_data
    task_type: ExtractOperator
    http_conn_id: alphavantage_api_key
    endpoint: 'https://www.alphavantage.co/query'
    output_file: '/path/to/extracted/data.json'
    function: 'TIME_SERIES_QUARTERLY_ADJUSTED'
    symbol: 'MSFT'
  - task_id: transform_quarterly_financial_data
    task_type: TransformOperator
    input_file: '/path/to/extracted/data.json'
    output_file: '/path/to/transformed/data.parquet'
    spark_job: spark_jobs.reusable_spark_job.transform_data
    spark_config: spark_jobs/spark_configs.yaml
  - task_id: load_quarterly_financial_data
    task_type: LoadOperator
    input_data: '/path/to/transformed/data.parquet'
    db_conn_id: 'your_database_connection'
    destination_table: 'quarterly_financial_data'
dependencies:
  - upstream_task: extract_quarterly_financial_data
    downstream_task: transform_quarterly_financial_data
  - upstream_task: transform_quarterly_financial_data
    downstream_task: load_quarterly_financial_data
4. Update spark_configs.yaml to define the transformations to be applied to the extracted data. For example:
transformations:
  column1: uppercase
  column2: none
  column3: uppercase

required_columns:
  - column1
  - column2
  - column3
