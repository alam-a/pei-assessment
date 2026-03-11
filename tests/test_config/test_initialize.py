from random import randint
import pytest
from assessment.config.initialize import init_database, init_spark
from assessment.config.request_config import RequestConfig
import sys
from unittest.mock import patch
from pyspark.sql import DataFrame, SparkSession

@pytest.fixture
def request_config():
    with patch.object(sys, "argv", ["x.py", f"db=coke_dummy_db_{randint(1000, 9999)}_{randint(1000, 9999)}", "schema=uat", "input_location=/data/sources/"]):
        return RequestConfig()

def test_spark_initialization():
    # Test spark initialization, and then close the session
    spark = init_spark()
    assert spark is not None
    assert isinstance(spark.range(1), DataFrame)
    spark.stop()

def test_initialize_database(request_config, spark: SparkSession):
    # Clean up any existing database with the same name
    spark.sql(f"DROP DATABASE IF EXISTS {request_config.db} CASCADE")
    assert not spark.catalog.databaseExists(request_config.db)

    # Initialize the database
    init_database(spark, request_config)
    assert spark.catalog.databaseExists(request_config.db)
