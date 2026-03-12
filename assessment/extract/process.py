from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

from assessment.config.output_tables import OutputTables
from assessment.config.request_config import RequestConfig

from ..io.reader import read_csv, read_excel, read_json
from ..transform.utils import flatten_json_df
from ..io.writer import save_dataframe_as_table

def process(spark: SparkSession, request_config: RequestConfig):
    products_path = f"{request_config.input_location}/Products.csv"
    customers_path = f"{request_config.input_location}/Customer.xlsx"
    orders_path = f"{request_config.input_location}/Orders.json"

    products_df = normalize_column_names(read_csv(spark, products_path))
    products_df = strip_whitespace(products_df)
    customers_df = normalize_column_names(read_excel(spark, customers_path))
    customers_df = strip_whitespace(customers_df)
    orders_df = normalize_column_names(flatten_json_df(read_json(spark, orders_path)))
    orders_df = strip_whitespace(orders_df)

    output_tables = OutputTables(request_config.db)
    save_dataframe_as_table(products_df, output_tables.PRODUCT_EXTRACTS, request_config)
    save_dataframe_as_table(orders_df, output_tables.ORDER_EXTRACTS, request_config)
    save_dataframe_as_table(customers_df, output_tables.CUSTOMER_EXTRACTS, request_config)

def strip_whitespace(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim
    
    columns = [trim(col(column)).alias(column) for column in df.columns]
    return df.select(columns)

def normalize_column_names(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col
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