from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
import pytest
import pandas as pd

profit_aggregates = [
    {"customer_id": "TC-20980", "customer_name": "Tamara Chand", "year": 2014, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2014-01-03", "total_profit": 8470.07},
    {"customer_id": "TC-20981", "customer_name": "Tamara Chand", "year": 2014, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2014-01-07", "total_profit": 1470.07},
    {"customer_id": "AB-10105", "customer_name": "Adrian Barton", "year": 2015, "category": "Furniture", "sub_category": "Chairs", "order_date": "2015-01-03", "total_profit": 5492.89},
    {"customer_id": "HL-15040", "customer_name": "Hunter Lopez", "year": 2015, "category": "Technology", "sub_category": "Electronics", "order_date": "2015-01-03", "total_profit": 5196.15},
    {"customer_id": "SE-20110", "customer_name": "Sanjit Engle", "year": 2015, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2015-01-03", "total_profit": 3245.67},
    {"customer_id": "AB-20210", "customer_name": "Adrian Barton", "year": 2016, "category": "Furniture", "sub_category": "Chairs", "order_date": "2016-01-03", "total_profit": 5492.89},
    {"customer_id": "HL-15040", "customer_name": "Hunter Lopez", "year": 2015, "category": "Technology", "sub_category": "Electronics", "order_date": "2015-01-03", "total_profit": 5196.15},
    {"customer_id": "SE-20110", "customer_name": "Sanjit Engle", "year": 2014, "category": "Furniture", "sub_category": "Chairs", "order_date": "2014-01-03", "total_profit": 3245.67}
]

@pytest.fixture
def profit_aggregates_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(pd.DataFrame(profit_aggregates)).cache()