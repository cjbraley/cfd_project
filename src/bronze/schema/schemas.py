from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, StructType

schemas = dict()

schemas["simulation"] = StructType([
    StructField("simulation_id", StringType(), False),
    StructField("shape_id", StringType(), False),
    StructField("date", StringType(), False),
    StructField("design", StringType(), False),
    StructField("total_lift", FloatType(), False),
    StructField("total_drag", FloatType(), False),
    StructField("speed", FloatType(), False),
    StructField("velocity", StructType([
        StructField("x", FloatType(), False),
        StructField("y", FloatType(), False),
        StructField("z", FloatType(), False)
    ]), False),
    StructField("pressure", StructType([
        StructField("front", FloatType(), False),
        StructField("rear", FloatType(), False),
        StructField("top", FloatType(), False),
        StructField("bottom", FloatType(), False)
    ]), False),
    StructField("_ingestion_dt", TimestampType(), False),
    StructField("_source_system", StringType(), False),
    StructField("_source_file", StringType(), False),
])
