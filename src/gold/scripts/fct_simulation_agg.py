from pyspark.sql import functions as F
from src.gold.schema.fct_simulation_agg import schema

def fct_simulation_agg(spark, logger) -> None:
    df = spark.read.format("iceberg").load("silver.simulation")
    df = simulation_agg(df)
    df.write.format("iceberg").mode("overwrite").saveAsTable("gold.fct_simulation_agg")

def simulation_agg(df):
    df = (
        df
        .groupBy("date", "shape_id", "design")
        .agg(
            F.avg("total_lift").alias("avg_total_lift"),
            F.max("total_lift").alias("max_total_lift"),
            F.min("total_lift").alias("min_total_lift"),

            F.avg("total_drag").alias("avg_total_drag"),
            F.max("total_drag").alias("max_total_drag"),
            F.min("total_drag").alias("min_total_drag"),

            F.avg("lift_to_drag_ratio").alias("avg_lift_to_drag_ratio"),
            F.max("lift_to_drag_ratio").alias("max_lift_to_drag_ratio"),
            F.min("lift_to_drag_ratio").alias("min_lift_to_drag_ratio"),
            
            F.avg("speed").alias("avg_speed"),
            F.max("speed").alias("max_speed"),
            F.min("speed").alias("min_speed"),
            
            F.avg(F.expr("pressure_top - pressure_bottom")).alias("avg_pressure_delta_top_bottom"),
            F.max(F.expr("pressure_top - pressure_bottom")).alias("max_pressure_delta_top_bottom"),
            F.min(F.expr("pressure_top - pressure_bottom")).alias("min_pressure_delta_top_bottom"),

            F.count("*").alias("simulation_count")
        )
        .withColumn("_generated_dt", F.current_timestamp())
    )
    return df.select([F.col(field.name) for field in schema])