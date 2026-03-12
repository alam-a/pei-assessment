from pyspark.sql.dataframe import DataFrame

from assessment.io.reader import logger
from assessment.config.request_config import RequestConfig, RunModes

def save_dataframe_as_table(df: DataFrame, table: str, request_config: RequestConfig):
    logger.info(f"Saving dataframe to table {table}")
    table_name = table.split(".")[1]
    path = f"{request_config.db_location}/{table_name}"
    if request_config.run_mode == RunModes.LOCAL_MODE:
        df.write.mode("overwrite").option("path", path).saveAsTable(table)
    else:
        df.write.mode("overwrite").saveAsTable(table)

def append_dataframe_to_table(df: DataFrame, table: str, request_config: RequestConfig):
    logger.info(f"Appending dataframe to table {table}")
    if request_config.run_mode == RunModes.LOCAL_MODE:
        table_name = table.split(".")[1]
        path = f"{request_config.db_location}/{table_name}"
        df.write.mode("append").option("path", path).saveAsTable(table)
    else:
        df.write.mode("append").saveAsTable(table)
