from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import regexp_replace, col, to_date, monotonically_increasing_id, row_number, initcap
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import List, Optional, Dict, Any

from assessment.config import output_tables
from assessment.config.output_tables import OutputTables
from assessment.config.request_config import RequestConfig
from assessment.io.writer import save_dataframe_as_table

def clean_phone_numbers(column: Column) -> Column:
    return regexp_replace(column, "[^0-9]", "")

def deduplicate_rows(df: DataFrame, primary_keys: List[str]) -> DataFrame:
    if not primary_keys:
        raise ValueError("Primary keys list cannot be empty")
    
    # Create a window spec to identify duplicates
    window_spec = Window.partitionBy(*primary_keys).orderBy(col("temp_row_number"))
    
    # Add row number and filter to keep only first occurrence
    df_with_row_num = df.withColumn("temp_row_number", monotonically_increasing_id())
    df_with_duplicates = df_with_row_num.withColumn("temp_duplicate_rank", row_number().over(window_spec))
    
    # Filter to keep only the first occurrence (rank = 1)
    deduplicated_df = df_with_duplicates.filter(col("temp_duplicate_rank") == 1).drop("temp_duplicate_rank", "temp_row_number")
    
    return deduplicated_df

def deduplicate_using_aggregation(df: DataFrame, primary_keys: List[str], aggregation_rules: Optional[Dict[str, str]] = None) -> DataFrame:
    """
    Remove duplicate rows using aggregation. Maybe useful for things like price, where we may want average price
    """
    if not primary_keys:
        raise ValueError("Primary keys list cannot be empty")
    
    if aggregation_rules is None:
        logger.warning("Aggregation rules is None, falling back to simple deduplication")
        return deduplicate_rows(df, primary_keys)
    
    from pyspark.sql.functions import first, last, max, min, sum, avg, count
    aggregation_function_mapping = {
        "first": first,
        "last": last,
        "max": max,
        "min": min,
        "sum": sum,
        "avg": avg,
        "count": count,
    }

    # Get non-key columns
    non_key_columns = [col for col in df.columns if col not in primary_keys]
    aggregation_functions = {
        column: aggregation_function_mapping[aggregation_rules.get(column, "first")] for column in non_key_columns
    }
    
    # Build aggregation expressions - use first as default for primary keys - we can also let it fail here for unsupported aggregation functions
    agg_cols = [aggregation_functions.get(column, first)(col(column)).alias(column) for column in non_key_columns]
    
    # Group by primary keys and aggregate
    result_df = df.groupBy(*primary_keys).agg(*agg_cols)
    
    return result_df

def convert_date_str_to_date(date_column: Column, input_format: str = "dd/MM/yyyy") -> Column:
    """
    Convert date string to date.
    """
    return to_date(date_column, input_format)

def convert_name_to_alphabetic_with_name_case(name_column: Column) -> Column:
    # Remove non-alphabetic characters (keep only letters and spaces)
    cleaned = regexp_replace(name_column, r"[^a-zA-Z\s]", "")
    return initcap(cleaned)
    
def flatten_json_df(df: DataFrame) -> DataFrame:
    # Recursively flatten a nested dataframe, such as one loaded from JSON
    # We could limit the levels of recursion if needed, but I have not implemented that yet
    from pyspark.sql.types import ArrayType, StructType
    change_needed = True
    while change_needed:
        change_needed = False
        for col in df.schema.fields:
            if isinstance(col.dataType, ArrayType):
                df = df.withColumn(col.name, F.explode(col.name))
                change_needed = True
            if isinstance(col.dataType, StructType):
                for field in col.dataType.fields:
                    df = df.withColumn(f"{col.name}_{field.name}", F.col(f"{col.name}.{field.name}"))
                df = df.drop(col.name)
                change_needed = True
    return df


def create_enriched_customers_products_table(customers_df: DataFrame, products_df: DataFrame):
    
    # Create enriched customers table
    enriched_customers = customers_df.select(
        "customer_id", 
        "customer_name", 
        "country",
        "phone"
    )
    
    # Create enriched products table  
    enriched_products = products_df.select(
        "product_id",
        "category", 
        "sub_category",
        "product_name",
        "price_per_product"
    )
    
    return enriched_customers, enriched_products


def create_enriched_orders(customers_df: DataFrame, orders_df: DataFrame, products_df: DataFrame):
    """Create enriched table with order information, profit, customer details, and product details"""
    print("Creating enriched orders table...")
    from pyspark.sql.functions import round

    # Join orders with customers and products
    enriched_orders = orders_df \
        .join(customers_df, "customer_id", "left") \
        .join(products_df, "product_id", "left") \
        .select(
            orders_df["order_id"],
            orders_df["order_date"], 
            orders_df["ship_date"],
            orders_df["ship_mode"],
            orders_df["customer_id"],
            customers_df["customer_name"],
            customers_df["country"],
            orders_df["product_id"],
            products_df["category"],
            products_df["sub_category"], 
            products_df["product_name"],
            orders_df["quantity"],
            orders_df["price"],
            orders_df["discount"],
            round(orders_df["profit"], 2).alias("profit")
        )\
        .withColumn("category", F.when((col("category").isNull()) | (col("category") == "") | (col("category") == "NULL"), "Unknown Category").otherwise(col("category")))\
        .withColumn("sub_category", F.when((col("sub_category").isNull()) | (col("sub_category") == "") | (col("sub_category") == "NULL"), "Unknown Sub Category").otherwise(col("sub_category")))\
        .withColumn("product_name", F.when((col("product_name").isNull()) | (col("product_name") == "") | (col("product_name") == "NULL"), "Unknown Product Name").otherwise(col("product_name")))\
        .withColumn("customer_name", F.when((col("customer_name").isNull()) | (col("customer_name") == "") | (col("customer_name") == "NULL"), "Unknown Customer Name").otherwise(col("customer_name")))\
        .withColumn("country", F.when((col("country").isNull()) | (col("country") == "") | (col("country") == "NULL"), "Unknown Country").otherwise(col("country")))
    return enriched_orders

def create_profit_aggregates(spark: SparkSession, enriched_orders_df: DataFrame) -> DataFrame:
    """Create aggregate table showing profit by Year, Product Category, Product Sub Category, Customer"""
    print("Creating aggregate table...")
    
    # Create aggregate table with multiple dimensions
    aggregate_df = enriched_orders_df \
        .withColumn("year", F.year(col("order_date"))) \
        .groupBy("year", "category", "sub_category", "customer_id", "customer_name") \
        .agg(F.round(F.sum("profit"), 2).alias("total_profit"))
        
    return aggregate_df
