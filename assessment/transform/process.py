from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from logging import getLogger
from assessment.transform.utils import deduplicate_rows, deduplicate_using_aggregation, convert_date_str_to_date
from pyspark.sql.functions import col, round
from assessment.transform.utils import convert_name_to_alphabetic_with_name_case, clean_phone_numbers

def process(spark: SparkSession, base_path: str) -> DataFrame:
    logger = getLogger(__name__)
    logger.info("Transforming data...")
    orders_path = f"{base_path}/orders.parquet"
    products_path = f"{base_path}/products.parquet"
    customers_path = f"{base_path}/customers.parquet"
    orders_df = spark.read.parquet(orders_path)
    products_df = spark.read.parquet(products_path)
    customers_df = spark.read.parquet(customers_path)

    orders_df = deduplicate_rows(orders_df, ["order_id"])\
        .withColumn("order_date", convert_date_str_to_date(col("order_date"), "d/M/yyyy"))\
        .withColumn("ship_date", convert_date_str_to_date(col("ship_date"), "d/M/yyyy"))\
        .withColumn("quantity", col("quantity").cast("int"))\
        .withColumn("price", round(col("price").cast("float"), 2))\
        .withColumn("discount", round(col("discount").cast("float"), 2))\
        .withColumn("profit", round(col("profit").cast("float"), 2))
    logger.info(f"Saving orders df after deduplication and cleanup")
    orders_df.write.mode("overwrite").parquet(f"{base_path}/orders_transformed.parquet")
    
    # need to check if taking avg of price is a valid strategy, maybe take max instead?
    products_df = deduplicate_using_aggregation(products_df, ["product_id"], {"price_per_product": "avg"})
    logger.info(f"Saving products df after deduplication")
    products_df.write.mode("overwrite").parquet(f"{base_path}/products_transformed.parquet")

    customers_df = deduplicate_rows(customers_df, ["customer_id"])\
        .withColumn("customer_name", convert_name_to_alphabetic_with_name_case(col("customer_name")))\
        .withColumn("phone", clean_phone_numbers(col("phone")))
    logger.info(f"Saving customers df after deduplication and cleanup")
    customers_df.write.mode("overwrite").parquet(f"{base_path}/customers_transformed.parquet")
    
    # Create enriched tables
    from assessment.transform.utils import create_enriched_customers_products_table, create_enriched_orders_table
    create_enriched_customers_products_table(spark, base_path)
    create_enriched_orders_table(spark, base_path)
