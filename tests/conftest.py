import pytest
from pyspark.sql.session import SparkSession

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName("pei assesment").getOrCreate()

@pytest.fixture
def sample_data():
    return [{"first_name": "Salman", "second_name": "Khan", "age": 55}]

def test_spark_session_fixture(spark):
    assert spark.range(100).count == 100