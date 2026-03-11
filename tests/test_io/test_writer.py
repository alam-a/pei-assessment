from pyspark.sql.session import SparkSession
from assessment.io.writer import save_dataframe_as_table, append_dataframe_to_table
import pyspark.sql.functions as F

import pytest

"""
    Test database with fixture test_db_name is setup in conftest.py
"""

@pytest.fixture(scope="module")
def test_table_name(spark, test_db_name):
    # Clean up any existing table
    spark.sql(f"DROP TABLE IF EXISTS {test_db_name}.test_table")
    yield f"{test_db_name}.test_table"
    # Clean up after test
    spark.sql(f"DROP TABLE IF EXISTS {test_db_name}.test_table")

def test_save_dataframe_as_table(spark: SparkSession, test_table_name: str):
    df = spark.range(10)
    save_dataframe_as_table(df, test_table_name)
    assert spark.table(test_table_name).count() == 10

def test_append_dataframe_to_table(spark: SparkSession, test_table_name: str):
    df = spark.range(10)
    save_dataframe_as_table(df, test_table_name)
    assert spark.table(test_table_name).count() == 10

    append_dataframe_to_table(df, test_table_name)
    assert spark.table(test_table_name).count() == 20

