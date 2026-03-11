from pyspark.sql.session import SparkSession

from assessment.config.request_config import RequestConfig

def init_spark():
    spark = SparkSession.builder \
        .appName("PEI Assessment Data Pipeline") \
        .master("local[*]") \
        .getOrCreate()
    # .config("spark.sql.warehouse.dir", "spark-warehouse") \
    return spark

def init_database(spark: SparkSession, request_config: RequestConfig):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {request_config.db}")