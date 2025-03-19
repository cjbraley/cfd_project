import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from src.bronze.scripts.load import add_metadata

def test_add_metadata(spark: SparkSession):
    # EXPECTED
    expected_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("_ingestion_dt", TimestampType(), True),
        StructField("_source_system", StringType(), True),
        StructField("_source_file", StringType(), True),
    ])
    expected_data = [
        ("1", "Alice", datetime(2023, 10, 26, 10, 0, 0), "file_system", ""),
        ("2", "Bob", datetime(2023, 10, 26, 10, 0, 0), "file_system", ""),
    ]
    df_expected = spark.createDataFrame(expected_data, expected_schema)

    # INPUT
    input_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ])
    input_data = [("1", "Alice"), ("2", "Bob")]
    df_input = spark.createDataFrame(input_data, input_schema)

    # RESULT
    df_result = add_metadata(df_input)

    # check timestamp is being generated
    assert "_ingestion_dt" in df_result.columns

    # adjust timestamp because it will never match
    df_result_manual_timestamp = df_result.withColumn("_ingestion_dt", F.to_timestamp(F.lit("2023-10-26T10:00:00")))
    

    # ASSERT
    assert sorted(df_result_manual_timestamp.collect()) == sorted(df_expected.collect())