import pytest
from pyspark.sql.session import SparkSession
from assessment.transform.process import process
import pandas as pd
import os
from tests.test_transform.sample_integration_data import orders, products, customers

@pytest.fixture
def data_path(tmp_path):
    print(os.getcwd())
    pd.DataFrame(orders).to_parquet(tmp_path / "orders.parquet")
    pd.DataFrame(products).to_parquet(tmp_path / "products.parquet")
    pd.DataFrame(customers).to_parquet(tmp_path / "customers.parquet")
    return tmp_path

def test_process(spark: SparkSession, data_path):
    process(spark, data_path)
    assert os.path.exists(data_path / "orders_transformed.parquet")
    assert os.path.exists(data_path / "products_transformed.parquet")
    assert os.path.exists(data_path / "customers_transformed.parquet")
    assert spark.read.parquet(str(data_path / "orders_transformed.parquet")).count() > 0
    assert spark.read.parquet(str(data_path / "customers_transformed.parquet")).count() > 0
    assert spark.read.parquet(str(data_path / "products_transformed.parquet")).count() > 0
    assert spark.read.parquet(str(data_path / "products_transformed.parquet")).count() < spark.read.parquet(str(data_path / "products.parquet")).count()
