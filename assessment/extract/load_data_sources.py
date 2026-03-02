from pyspark.sql import Column
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pathlib import Path

from ..io.reader import read_csv, read_excel, read_json
from ..transform.utils import flatten_json_df


def load(spark: SparkSession, base_path: str):
    products_path = f"{base_path}/Products.csv"
    customers_path = f"{base_path}/Customer.xlsx"
    orders_path = f"{base_path}/Orders.json"

    products_df = normalize_column_names(read_csv(spark, products_path))
    products_df = strip_whitespace(products_df)
    customers_df = normalize_column_names(read_excel(spark, customers_path))
    customers_df = strip_whitespace(customers_df)
    orders_df = normalize_column_names(flatten_json_df(read_json(spark, orders_path)))
    orders_df = strip_whitespace(orders_df)

    products_df.write.mode("overwrite").parquet(f"{base_path}/outputs/products.parquet")
    orders_df.write.mode("overwrite").parquet(f"{base_path}/outputs/orders.parquet")
    customers_df.write.mode("overwrite").parquet(f"{base_path}/outputs/customers.parquet")

def strip_whitespace(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim
    
    columns = [trim(col(column)).alias(column) for column in df.columns]
    return df.select(columns)

def normalize_column_names(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col
    from pyspark.sql.types import StringType

    import re
    def update_column(column: str) -> str:
        column = column.strip()
        column = re.sub(r"[^a-zA-Z0-9_]", "_", column)
        column = column.replace("__", "_")
        if column[0].isdigit():
            column = column[1:]
        return column.lower()
    
    columns = [col(column).alias(update_column(column)) for column in df.columns]
    return df.select(*columns)