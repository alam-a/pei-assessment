import pytest
from pyspark.sql import SparkSession
import pandas as pd
from pathlib import Path
import os

sample_enriched_orders_data = [
    {"customer_id": "TC-20980", "customer_name": "Tamara Chand", "year": 2014, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2014-01-03", "profit": 8470.07},
    {"customer_id": "TC-20980", "customer_name": "Tamara Chand", "year": 2014, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2014-01-07", "profit": 1470.07},
    {"customer_id": "AB-10105", "customer_name": "Adrian Barton", "year": 2014, "category": "Furniture", "sub_category": "Chairs", "order_date": "2014-01-03", "profit": 5492.89},
    {"customer_id": "HL-15040", "customer_name": "Hunter Lopez", "year": 2014, "category": "Technology", "sub_category": "Electronics", "order_date": "2014-01-03", "profit": 5196.15},
    {"customer_id": "SE-20110", "customer_name": "Sanjit Engle", "year": 2014, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2014-01-03", "profit": 3245.67}
]

@pytest.fixture
def enriched_orders_data_path(tmp_path):
    print(os.getcwd())
    pd.DataFrame(sample_enriched_orders_data).to_parquet(tmp_path / "enriched" / "enriched_orders.parquet")
    return tmp_path

def test_aggregated_orders(spark: SparkSession, enriched_orders_data_path):

    assert Path(enriched_orders_data_path).exists()
    from assessment.load.aggregates import create_aggregate_table
    create_aggregate_table(spark, enriched_orders_data_path)

    assert (Path(enriched_orders_data_path) / "aggregate" / "profit_aggregates.parquet").exists()
    result = spark.read.parquet(str(Path(enriched_orders_data_path) / "aggregate" / "profit_aggregates.parquet"))
    assert result.count() == 4
