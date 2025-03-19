# great_expectations/expectations/bronze/basic_expectations.py
from great_expectations.dataset import SparkDFDataset

def create_bronze_expectations(df):
    ge_df = SparkDFDataset(df)
    ge_df.expect_column_values_to_not_be_null("id")
    ge_df.expect_column_values_to_not_be_null("timestamp")
    return ge_df