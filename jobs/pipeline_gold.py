from src.utils.Pipeline import Pipeline
from src.gold.scripts.fct_simulation_agg import fct_simulation_agg

tasks = [
    fct_simulation_agg
]

with Pipeline(app_name="pipeline_gold") as pipeline:
    for task in tasks:
        task(pipeline.spark, pipeline.logger)