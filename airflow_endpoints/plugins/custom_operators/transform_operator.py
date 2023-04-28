# airflow/plugins/custom_operators/TransformOperator.py

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any
from pyspark.sql import SparkSession
from spark_jobs.reusable_spark_job import transform_data


class TransformOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            input_data: str,
            output_data: str,
            spark_job_config: str,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.input_data = input_data
        self.output_data = output_data
        self.spark_job_config = spark_job_config

    def execute(self, context: Dict[str, Any]) -> None:
        # Create a Spark session
        spark = SparkSession.builder \
            .appName("ETL_Transform") \
            .getOrCreate()

        # Run the transformation logic
        transform_data(spark, self.input_data, self.output_data, self.spark_job_config)

        # Stop the Spark session
        spark.stop()
