import sys
import os
from datetime import date, datetime
from pyspark.sql import SparkSession, functions as F


# fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from src.gold.scripts.fct_simulation_agg import simulation_agg
from src.gold.schema.fct_simulation_agg import schema as expected_schema
from src.silver.schema.simulation import schema as input_schema



def test_transform_simulation(spark: SparkSession):

    # EXPECTED
    expected_data = [
        (
            date(2023, 11, 1), "shape_A", "design_1", 25.0, 30.0, 20.0, 6.0, 7.0, 5.0, 5.0, 6.0, 4.0, 11.0, 12.0, 10.0, 6.0, 7.0, 5.0, 2, datetime(2023, 11, 1, 10, 0, 0)
        ),
        (
            date(2023, 11, 2), "shape_B", "design_2", 35.0, 40.0, 30.0, 7.0, 8.0, 6.0, 6.0, 7.0, 5.0, 13.0, 14.0, 12.0, 5.5, 7.0, 4.0, 2, datetime(2023, 11, 1, 10, 0, 0)
        )
    ]

    df_expected = spark.createDataFrame(expected_data, expected_schema)

    # INPUT
    input_data = [
        (
            "sim_1", "shape_A", "design_1", date(2023, 11, 1), 10.0, 1.0, 2.0, 3.0, 20.0, 5.0, 10.0, 5.0, 15.0, 8.0, 4.0, 7.0, 5.0, datetime(2023, 11, 1, 10, 0, 0), "source_1", "file_1.csv"
        ),
        (
            "sim_2", "shape_B", "design_2", date(2023, 11, 2), 12.0, 4.0, 5.0, 6.0, 30.0, 6.0, 12.0, 6.0, 16.0, 9.0, 5.0, 7.0, 6.0, datetime(2023, 11, 2, 12, 30, 0), "source_2", "file_2.csv"
        ),
        (
            "sim_3", "shape_A", "design_1", date(2023, 11, 1), 12.0, 2.0, 3.0, 4.0, 30.0, 7.0, 14.0, 9.0, 9.0, 4.0, 6.0, 9.0, 7.0, datetime(2023, 11, 1, 11, 0, 0), "source_3", "file_3.csv"
        ),
        (
             "sim_4", "shape_B", "design_2", date(2023, 11, 2), 14.0, 5.0, 6.0, 7.0, 40.0, 8.0, 16.0, 10.0, 12.0, 8.0, 7.0, 13.0, 12.0, datetime(2023, 11, 2, 13, 30, 0), "source_4", "file_4.csv"
        )
    ]

    df_input = spark.createDataFrame(input_data, input_schema)

    # RESULT
    df_result = simulation_agg(df_input)

    # check timestamp is being generated
    assert "_generated_dt" in df_result.columns

    # adjust timestamp because it will never match
    df_result_manual_timestamp = df_result.withColumn("_generated_dt", F.to_timestamp(F.lit(datetime(2023, 11, 1, 10, 0, 0))))
    

    # ASSERT
    assert sorted(df_result_manual_timestamp.collect()) == sorted(df_expected.collect())