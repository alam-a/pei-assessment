import pytest
from pyspark.sql.session import SparkSession

def pytest_configure(config):
    import os
    os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName("pei assesment").getOrCreate()

@pytest.fixture
def sample_data():
    return [{"first_name": "Salman", "second_name": "Khan", "age": 55}]

@pytest.fixture
def sample_uncleaned_headers():
    return [{"first Name": "Salman", "Second Name": "Khan", "age": 55}]

@pytest.fixture
def sample_orders_data():
    sample_json = [
        {
            "Row ID": 1,
            "Order ID": "CA-2016-122581",
            "Order Date": "21/8/2016",
            "Ship Date": "25/8/2016",
            "Ship Mode": "Standard Class",
            "Customer ID": "JK-15370",
            "Product ID": "FUR-CH-10002961",
            "Quantity": 7,
            "Price": 573.17,
            "Discount": 0.3,
            "Profit": 63.69
        },
        {
            "Row ID": 2,
            "Order ID": "CA-2017-117485",
            "Order Date": "23/9/2017",
            "Ship Date": "29/9/2017",
            "Ship Mode": "Standard Class",
            "Customer ID": "BD-11320",
            "Product ID": "TEC-AC-10004659",
            "Quantity": 4,
            "Price": 291.96,
            "Discount": 0,
            "Profit": 102.19
        }
    ]
    return sample_json

@pytest.fixture
def sample_customers_data():
    return [
        {
            "Customer ID": "JK-15370",
            "Customer Name": "John Smith",
            "Segment": "Consumer",
            "City": "New York",
            "State": "New York",
            "Country": "United States",
            "Postal Code": "10001",
            "Region": "Northeast"
        }
    ]

@pytest.fixture(scope="session")
def test_spark_session_fixture(spark):
    assert spark.range(100).count == 100