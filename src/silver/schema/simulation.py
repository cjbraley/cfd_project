from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, TimestampType, IntegerType

schema = StructType([
    StructField("simulation_id", StringType(), False),  # False means not nullable
    StructField("shape_id", StringType(), True),
    StructField("design", StringType(), True),
    StructField("date", DateType(), True),
    StructField("speed", FloatType(), True),
    StructField("velocity_x", FloatType(), True),
    StructField("velocity_y", FloatType(), True),
    StructField("velocity_z", FloatType(), True),
    StructField("total_lift", FloatType(), True),
    StructField("total_drag", FloatType(), True),
    StructField("pressure_front", FloatType(), True),
    StructField("pressure_rear", FloatType(), True),
    StructField("pressure_top", FloatType(), True),
    StructField("pressure_bottom", FloatType(), True),
    StructField("lift_to_drag_ratio", FloatType(), True),
    StructField("pressure_delta_top_bottom", FloatType(), True),
    StructField("pressure_delta_front_rear", FloatType(), True),
    StructField("_ingestion_dt", TimestampType(), False),
    StructField("_source_system", StringType(), False),
    StructField("_source_file", StringType(), False)
])