from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from logging import getLogger
from assessment.config.request_config import RequestConfig
from assessment.config.output_tables import OutputTables
from assessment.transform.utils import deduplicate_rows, deduplicate_using_aggregation, convert_date_str_to_date
from pyspark.sql.functions import col, round
from assessment.transform.utils import convert_name_to_alphabetic_with_name_case, clean_phone_numbers
from assessment.io.writer import save_dataframe_as_table 

def process(spark: SparkSession, request_config: RequestConfig):
    logger = getLogger(__name__)
    logger.info("Transforming data...")
    output_tables = OutputTables(request_config.db)

    orders_df = spark.table(output_tables.ORDER_EXTRACTS)
    products_df = spark.table(output_tables.PRODUCT_EXTRACTS)
    customers_df = spark.table(output_tables.CUSTOMER_EXTRACTS)

    orders_df = deduplicate_rows(orders_df, ["order_id"])\
        .withColumn("order_date", convert_date_str_to_date(col("order_date"), "d/M/yyyy"))\
        .withColumn("ship_date", convert_date_str_to_date(col("ship_date"), "d/M/yyyy"))\
        .withColumn("quantity", col("quantity").cast("int"))\
        .withColumn("price", round(col("price").cast("float"), 2))\
        .withColumn("discount", round(col("discount").cast("float"), 2))\
        .withColumn("profit", round(col("profit").cast("float"), 2))
    logger.info(f"Saving orders df after deduplication and cleanup")
    save_dataframe_as_table(orders_df, table=output_tables.ORDER_TRANSFORMED, request_config=request_config)
    
    # need to check if taking avg of price is a valid strategy, maybe take max instead?
    products_df = deduplicate_using_aggregation(products_df, ["product_id"], {"price_per_product": "avg"})
    logger.info(f"Saving products df after deduplication")
    save_dataframe_as_table(products_df, table=output_tables.PRODUCT_TRANSFORMED, request_config=request_config)

    customers_df = deduplicate_rows(customers_df, ["customer_id"])\
        .withColumn("customer_name", convert_name_to_alphabetic_with_name_case(col("customer_name")))\
        .withColumn("phone", clean_phone_numbers(col("phone")))
    logger.info(f"Saving customers df after deduplication and cleanup")
    save_dataframe_as_table(customers_df, table=output_tables.CUSTOMER_TRANSFORMED, request_config=request_config)

    customers_df = spark.table(output_tables.CUSTOMER_TRANSFORMED)
    products_df = spark.table(output_tables.PRODUCT_TRANSFORMED)
    orders_df = spark.table(output_tables.ORDER_TRANSFORMED)
    
    # Create enriched tables
    from assessment.transform.utils import create_enriched_customers_products_table, create_enriched_orders, create_profit_aggregates
    enriched_customers, enriched_products = create_enriched_customers_products_table(
        customers_df=customers_df, products_df=products_df
    )
    save_dataframe_as_table(enriched_customers, table=output_tables.ENRICHED_CUSTOMERS, request_config=request_config)
    save_dataframe_as_table(enriched_products, table=output_tables.ENRICHED_PRODUCTS, request_config=request_config)

    enriched_orders = create_enriched_orders(orders_df=orders_df,
        customers_df=customers_df, products_df=products_df)
    save_dataframe_as_table(enriched_orders, table=output_tables.ENRICHED_ORDERS, request_config=request_config)

    profit_aggregates = create_profit_aggregates(spark=spark, enriched_orders_df=spark.table(output_tables.ENRICHED_ORDERS))
    save_dataframe_as_table(profit_aggregates, table=output_tables.PROFIT_AGGREGATES, request_config=request_config)
