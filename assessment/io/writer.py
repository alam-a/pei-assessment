from pyspark.sql.dataframe import DataFrame

from assessment.io.reader import logger

def save_dataframe_as_table(df: DataFrame, table: str):
    logger.info(f"Saving dataframe to table {table}")
    df.write.mode("overwrite").saveAsTable(table)

def append_dataframe_to_table(df: DataFrame, table: str):
    logger.info(f"Appending dataframe to table {table}")
    df.write.mode("append").saveAsTable(table)