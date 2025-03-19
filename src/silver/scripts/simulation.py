from pyspark.sql import functions as F
from src.silver.schema.simulation import schema

def simulation(spark, logger):
    df = spark.read.format("iceberg").load("bronze.simulation")
    df = transform_simulation(df)
    df.write.format("iceberg").mode("overwrite").saveAsTable("silver.simulation")

def transform_simulation(df):
    df = (
        df
        # flatten structs
        .withColumn("velocity_x", F.col("velocity.x"))
        .withColumn("velocity_y", F.col("velocity.y"))
        .withColumn("velocity_z", F.col("velocity.z"))
        .withColumn("pressure_front", F.col("pressure.front"))
        .withColumn("pressure_rear", F.col("pressure.rear"))
        .withColumn("pressure_top", F.col("pressure.top"))
        .withColumn("pressure_bottom", F.col("pressure.bottom"))
        
        # derived columns
        .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn(
            "lift_to_drag_ratio",
            F.round(F.col("total_lift") / F.col("total_drag"), 2)
        )
        .withColumn(
            "pressure_delta_top_bottom",
            F.round(F.col("pressure_top") - F.col("pressure_bottom"), 2)
        )
        .withColumn(
            "pressure_delta_front_rear",
            F.round(F.col("pressure_front") - F.col("pressure_rear"), 2)
        )
    )

    # Select only the columns defined in the schema
    return df.select([F.col(field.name) for field in schema])