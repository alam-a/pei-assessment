from pyspark.sql.session import SparkSession

from assessment.config.request_config import RequestConfig
from assessment.config.run_modes import RunModes

def init_spark(request_config: RequestConfig):
    spark_builder = (
        SparkSession.builder
            .appName("PEI Assessment Data Pipeline")
            .config("spark.sql.warehouse.dir", request_config.db_location)
        )
    if request_config.run_mode == RunModes.LOCAL_MODE:
        spark_builder = spark_builder.master("local[*]")
    return spark_builder.getOrCreate()

def init_database(spark: SparkSession, request_config: RequestConfig):
    if request_config.run_mode == RunModes.LOCAL_MODE:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {request_config.db} LOCATION '{request_config.db_location}'")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {request_config.db}")