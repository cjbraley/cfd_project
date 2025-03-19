import logging
import sys

from pyspark.sql import SparkSession

class Pipeline:
    """
    A class for managing Spark data processing pipelines.

    Provides core functionality like creating a SparkSession,setting up logging, 
    and handling pipeline execution. 

    Params:
        app_name (str): The name of the Spark application.
        log_level (str): The logging level (e.g., "INFO", "DEBUG", "ERROR").
    """

    def __init__(self, app_name: str, log_level: str = "INFO"):
        self.app_name = app_name
        self.log_level = log_level
        self.spark = None
        self.logger = None

    def _create_spark_session(self) -> SparkSession:
        return (
            SparkSession
            .builder
            .appName(self.app_name)
            .getOrCreate()
        )

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.app_name)
        logger.setLevel(self.log_level)

        # add logs to console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger

    def __enter__(self):
        self.spark = self._create_spark_session()
        self.logger = self._setup_logger()
        self.logger.info(f"Starting pipeline: {self.app_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped.")
        if exc_type:
            self.logger.error(f"Pipeline encountered an error: {exc_val}")
            return False  
        self.logger.info(f"Pipeline completed: {self.app_name}")
        return True