from pyspark.sql.session import SparkSession

from assessment.config.request_config import RequestConfig

def init_spark(request_config: RequestConfig):
    spark = (
        SparkSession.builder
            .appName("PEI Assessment Data Pipeline")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", request_config.db_location)
            .getOrCreate()
    )
    return spark

def init_database(spark: SparkSession, request_config: RequestConfig):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {request_config.db} LOCATION '{request_config.db_location}'")