from pyspark.sql import SparkSession, DataFrame, functions as F
import logging

from src.bronze.schema.schemas import schemas

def bronze_load(spark: SparkSession, logger: logging.Logger) -> bool:
    for table_name in schemas.keys():
        df: DataFrame = spark.read.json(f"/home/iceberg/data/{table_name}.json", schema=schemas[table_name])
        df = add_metadata(df)
        df.write.format("iceberg").mode("overwrite").saveAsTable(f"bronze.{table_name}")
    return True


def add_metadata(df: DataFrame) -> DataFrame:
    """
    Adds metadata columns to a DataFrame.

    Args:
        df: The input DataFrame.

    Returns:
        The DataFrame with metadata columns added.
    """
    return (
        df
        .withColumn("_ingestion_dt", F.current_timestamp())
        .withColumn("_source_system", F.lit("file_system"))
        .withColumn("_source_file", F.input_file_name())
    )