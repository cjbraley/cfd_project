from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, TimestampType, IntegerType

schema = StructType([
    StructField("date", DateType(), False),
    StructField("shape_id", StringType(), False),
    StructField("design", StringType(), False),
    StructField("avg_total_lift", FloatType(), False),
    StructField("max_total_lift", FloatType(), False),
    StructField("min_total_lift", FloatType(), False),
    StructField("avg_total_drag", FloatType(), False),
    StructField("max_total_drag", FloatType(), False),
    StructField("min_total_drag", FloatType(), False),
    StructField("avg_lift_to_drag_ratio", FloatType(), False),
    StructField("max_lift_to_drag_ratio", FloatType(), False),
    StructField("min_lift_to_drag_ratio", FloatType(), False),
    StructField("avg_speed", FloatType(), False),
    StructField("max_speed", FloatType(), False),
    StructField("min_speed", FloatType(), False),
    StructField("avg_pressure_delta_top_bottom", FloatType(), False),
    StructField("max_pressure_delta_top_bottom", FloatType(), False),
    StructField("min_pressure_delta_top_bottom", FloatType(), False),
    StructField("simulation_count", IntegerType(), False),
    StructField("_generated_dt", TimestampType(), False)
])