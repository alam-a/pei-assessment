from pyspark.sql import Column
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pathlib import Path

from ..io.reader import read_csv, read_excel, read_json
from ..transform.utils import flatten_json_df


def load(spark: SparkSession, base_path: str):
    csv_path = f"{base_path}/Products.csv"
    excel_path = f"{base_path}/Customer.xlsx"
    json_path = f"{base_path}/Orders.csv"

    csv_df = normalize_column_names(read_csv(spark, csv_path))
    csv_df = strip_whitespace(csv_df)
    excel_df = normalize_column_names(read_excel(spark, excel_path))
    excel_df = strip_whitespace(excel_df)
    json_df = normalize_column_names(flatten_json_df(read_json(spark, json_path)))
    json_df = strip_whitespace(json_df)

    csv_df.write.parquet(f"{base_path}/outputs/csv.parquet")
    json_df.write.parquet(f"{base_path}/outputs/json.parquet")
    excel_df.write.parquet(f"{base_path}/outputs/excel.parquet")

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