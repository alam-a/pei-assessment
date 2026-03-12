from pyspark.sql.session import SparkSession
import pandas as pd
import pytest
from pyspark.sql.functions import col

customers_transformed = [
    {"customer_id":"AA-10315","customer_name":"Alex Avila","email":"josephrice131@gmail.com","phone":"6802612092","address":"91773 Miller Shoal\nDiaztown, FL 38841","segment":"Consumer","country":"United States","city":"Round Rock","state":"Texas","postal_code":"78664","region":"Central"},
    {"customer_id":"AA-10375","customer_name":"Allen Armold","email":"garymoore386@gmail.com","phone":"22194541918872","address":"6450 John Lodge\nTerriton, KY 95945","segment":"Consumer","country":"United States","city":"Atlanta","state":"Georgia","postal_code":"30318","region":"South"},
    {"customer_id":"AA-10480","customer_name":"Andrew Allen","email":"johnwalker944@gmail.com","phone":"38814248835370","address":"27265 Murray Island\nKevinfort, PA 63231","segment":"Consumer","country":"United States","city":"Concord","state":"North Carolina","postal_code":"28027","region":"South"},
    {"customer_id":"AA-10645","customer_name":"Anna Andreadi","email":"ericcarter176@gmail.com","phone":"4512595402","address":"USNS Knight\nFPO AA 76532","segment":"Consumer","country":"United States","city":"Chester","state":"Pennsylvania","postal_code":"19013","region":"East"},
    {"customer_id":"AB-10015","customer_name":"Aaron Bergman","email":"williamjackson427@gmail.com","phone":"6256269133374","address":"170 Jackson Loaf\nKristenland, AS 48876","segment":"Consumer","country":"United States","city":"Arlington","state":"Texas","postal_code":"76017","region":"Central"}
]
product_transformed = [
    {"product_id":"FUR-BO-10000112","category":"Furniture","sub_category":"Bookcases","product_name":"Bush Birmingham Collection Bookcase, Dark Cherry","state":"Illinois","price_per_product":91.686},
    {"product_id":"FUR-CH-10000330","category":"Furniture","sub_category":"Chairs","product_name":"Custom Office Chair, Planked Cherry Finish","state":"Louisiana","price_per_product":120.98},
    {"product_id":"OS-SO-10000362","category":"Office Supplies","sub_category":"Storage","product_name":"Sauder Inglewood Storage Cabinet","state":"California","price_per_product":145.333},
    {"product_id":"OS-SO-10000468","category":"Office Supplies","sub_category":"Storage","product_name":"O'Sullivan 2-Shelf Heavy-Duty Storage Cabinet","state":"Washington","price_per_product":48.58},
    {"product_id":"CA-AC-10000711","category":"Technology","sub_category":"Accessories","product_name":"Mouse","state":"Arkansas","price_per_product":70.98}
]

orders_transformed = [
    {"customer_id":"AA-10315","discount":0.00,"order_date":'2014-07-08',"order_id":"CA-2014-100006","price":377.9700012207,"product_id":"FUR-BO-10000112","profit":109.6100006104,"quantity":3,"row_id":"7451","ship_date":1410566400000,"ship_mode":"Standard Class"},
    {"customer_id":"AA-10375","discount":0.20,"order_date":'2014-07-09',"order_id":"CA-2014-100090","price":196.6999969482,"product_id":"OS-SO-10000362","profit":68.8499984741,"quantity":6,"row_id":"303","ship_date":1405123200000,"ship_mode":"Standard Class"},
    {"customer_id":"AA-10480","discount":0.15,"order_date":'2015-07-08',"order_id":"CA-2014-100293","price":91.0599975586,"product_id":"OS-SO-10000362","profit":31.8700008392,"quantity":6,"row_id":"8016","ship_date":1395100800000,"ship_mode":"Standard Class"},
    {"customer_id":"AA-10645","discount":0.20,"order_date":'2016-07-08',"order_id":"CA-2014-100328","price":3.9300000668,"product_id":"CA-AC-10000711","profit":1.3300000429,"quantity":1,"row_id":"666","ship_date":1391385600000,"ship_mode":"Standard Class"},
    {"customer_id":"AB-10015","discount":0.33,"order_date":'2017-07-08',"order_id":"CA-2014-100363","price":19.0100002289,"product_id":"MISSING_PRODUCT_ID","profit":6.8899998665,"quantity":3,"row_id":"1074","ship_date":1397520000000,"ship_mode":"Standard Class"}
]

def test_create_enriched_customers_products_table(spark: SparkSession):
    from assessment.transform.utils import create_enriched_customers_products_table
    customers_df = spark.createDataFrame(pd.DataFrame(customers_transformed))
    products_df = spark.createDataFrame(pd.DataFrame(product_transformed))
    enriched_customers_df, enriched_products_df = create_enriched_customers_products_table(customers_df, products_df)
    assert enriched_customers_df.count() == len(customers_transformed)
    assert enriched_products_df.count() == len(product_transformed)

    #order matters, hence we need to compare as sets
    assert set(enriched_customers_df.columns) == {"customer_id", "customer_name", "country", "phone"}
    assert set(enriched_products_df.columns) == {"product_id", "category", "sub_category", "product_name", "price_per_product"}

def test_create_enriched_orders(spark: SparkSession):
    from assessment.transform.utils import create_enriched_orders
    customers_df = spark.createDataFrame(pd.DataFrame(customers_transformed))
    products_df = spark.createDataFrame(pd.DataFrame(product_transformed))
    orders_df = spark.createDataFrame(pd.DataFrame(orders_transformed))
    enriched_orders_df = create_enriched_orders(customers_df, orders_df, products_df)
    assert enriched_orders_df.count() >= len(orders_transformed) #(left join on orders, >= for if duplicate keys available)
    assert enriched_orders_df.filter(enriched_orders_df["category"] == 'Unknown Category').count() == 1 #(one order has no matching product)
    
    #order matters, hence we need to compare as sets
    assert set(enriched_orders_df.columns) == {"order_id", "order_date", "ship_date", "ship_mode", "quantity", "price", "discount", "profit", "customer_id", "customer_name", "country", "product_id", "product_name", "category", "sub_category"}


def test_profit_aggregated_orders(spark: SparkSession):
    from assessment.transform.utils import create_profit_aggregates
    sample_enriched_orders_data = [
        {"customer_id": "TC-20980", "customer_name": "Tamara Chand", "year": 2014, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2014-01-03", "profit": 8470.07},
        {"customer_id": "TC-20980", "customer_name": "Tamara Chand", "year": 2014, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2014-01-07", "profit": 1470.07},
        {"customer_id": "AB-10105", "customer_name": "Adrian Barton", "year": 2014, "category": "Furniture", "sub_category": "Chairs", "order_date": "2014-01-03", "profit": 5492.89},
        {"customer_id": "HL-15040", "customer_name": "Hunter Lopez", "year": 2014, "category": "Technology", "sub_category": "Electronics", "order_date": "2014-01-03", "profit": 5196.15},
        {"customer_id": "SE-20110", "customer_name": "Sanjit Engle", "year": 2014, "category": "Office Supplies", "sub_category": "Appliances", "order_date": "2014-01-03", "profit": 3245.67}
    ]
    enriched_order_df = spark.createDataFrame(pd.DataFrame(sample_enriched_orders_data))
    profit_aggregates = create_profit_aggregates(spark, enriched_order_df)

    assert enriched_order_df.count() == 5
    assert profit_aggregates.count() == 4
    assert set(profit_aggregates.columns) == set(["customer_id", "customer_name", "year", "category", "sub_category", "total_profit"])
    assert pytest.approx(profit_aggregates.filter(col("customer_id") == "TC-20980").collect()[0]["total_profit"]) == 9940.14
    assert pytest.approx(profit_aggregates.filter(col("customer_id") == "SE-20110").collect()[0]["total_profit"]) == 3245.67
