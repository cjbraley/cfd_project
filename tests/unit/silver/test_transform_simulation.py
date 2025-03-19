import sys
import os
from datetime import date, datetime
from pyspark.sql import SparkSession

# fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from src.silver.scripts.simulation import transform_simulation
from src.silver.schema.simulation import schema as expected_schema
from src.bronze.schema.schemas import schemas as input_schemas



def test_transform_simulation(spark: SparkSession):

    # EXPECTED
    expected_data = [
        (
            "sim_1", "shape_A", "design_1", date(2023, 11, 1), 10.0, 1.0, 2.0, 3.0, 20.0, 5.0, 10.0, 5.0, 15.0, 8.0, 4.0, 7.0, 5.0, datetime(2023, 11, 1, 10, 0, 0), "source_1", "file_1.csv"
        ),
        (
            "sim_2", "shape_B", "design_2", date(2023, 11, 2), 12.0, 4.0, 5.0, 6.0, 30.0, 6.0, 12.0, 6.0, 16.0, 9.0, 5.0, 7.0, 6.0, datetime(2023, 11, 2, 12, 30, 0), "source_2", "file_2.csv"
        ),
    ]
    df_expected = spark.createDataFrame(expected_data, expected_schema)

    # INPUT
    input_data = [
        (
            "sim_1", "shape_A", "2023-11-01T00:00:00", "design_1", 20.0, 5.0, 10.0, (1.0, 2.0, 3.0), (10.0, 5.0, 15.0, 8.0), datetime(2023, 11, 1, 10, 0, 0), "source_1", "file_1.csv"
        ),
        (
            "sim_2", "shape_B", "2023-11-02T00:00:00", "design_2", 30.0, 6.0, 12.0, (4.0, 5.0, 6.0), (12.0, 6.0, 16.0, 9.0), datetime(2023, 11, 2, 12, 30, 0), "source_2", "file_2.csv"
        ),
    ]

    df_input = spark.createDataFrame(input_data, input_schemas['simulation'])

    # RESULT
    df_result = transform_simulation(df_input)

    # ASSERT
    assert sorted(df_result.collect()) == sorted(df_expected.collect())