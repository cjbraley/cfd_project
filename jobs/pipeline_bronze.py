from src.utils.Pipeline import Pipeline
from src.bronze.scripts.load import bronze_load

with Pipeline(app_name="pipeline_bronze") as pipeline:
    bronze_load(pipeline.spark, pipeline.logger)