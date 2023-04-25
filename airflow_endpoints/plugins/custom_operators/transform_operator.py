# spark_jobs/jobs/reusable_spark_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(spark: SparkSession, input_data: str, output_data: str, config: dict):
    # Read the input data
    df = spark.read.json(input_data)

    # Apply transformations based on the config
    for column, transformation in config["transformations"].items():
        if transformation == "uppercase":
            df = df.withColumn(column, col(column).cast("string").upper())

    # Write the output data
    df.write.parquet(output_data)
