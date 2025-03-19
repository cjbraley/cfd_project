from src.utils.Pipeline import Pipeline
from src.silver.scripts.simulation import simulation

tasks = [
    simulation
]

with Pipeline(app_name="pipeline_silver") as pipeline:
    for task in tasks:
        task(pipeline.spark, pipeline.logger)