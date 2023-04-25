# spark_jobs/jobs/reusable_spark_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import yaml

def load_config(config_file: str) -> dict:
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def transform_data(spark: SparkSession, input_data: str, output_data: str, config_file: str):
    # Load the configuration
    config = load_config(config_file)

    # Read the input data
    df = spark.read.json(input_data)

    # Apply transformations based on the config
    for column, transformation in config["transformations"].items():
        if transformation == "uppercase":
            df = df.withColumn(column, col(column).cast("string").upper())

    # Write the output data
    df.write.parquet(output_data)
