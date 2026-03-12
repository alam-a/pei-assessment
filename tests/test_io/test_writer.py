from pyspark.sql.session import SparkSession
from assessment.io.writer import save_dataframe_as_table, append_dataframe_to_table
import pyspark.sql.functions as F

import pytest

"""
    Test database with fixture test_db_name is setup in conftest.py
"""

@pytest.fixture(scope="module")
def test_table_name(spark, test_request_config):
    # Clean up any existing table
    spark.sql(f"DROP TABLE IF EXISTS {test_request_config.db}.test_table")
    yield f"{test_request_config.db}.test_table"
    # Clean up after test
    spark.sql(f"DROP TABLE IF EXISTS {test_request_config.db}.test_table")

def test_save_dataframe_as_table(spark: SparkSession, test_table_name: str, test_request_config):
    df = spark.range(10)
    save_dataframe_as_table(df, test_table_name, test_request_config)
    assert spark.table(test_table_name).count() == 10
    
    import time
    time.sleep(10)
    
    save_dataframe_as_table(df, test_table_name, test_request_config)
    assert spark.table(test_table_name).count() == 10


def test_append_dataframe_to_table(spark: SparkSession, test_table_name: str, test_request_config):
    df = spark.range(10)
    save_dataframe_as_table(df, test_table_name, test_request_config)
    assert spark.table(test_table_name).count() == 10

    append_dataframe_to_table(df, test_table_name, test_request_config)
    assert spark.table(test_table_name).count() == 20

