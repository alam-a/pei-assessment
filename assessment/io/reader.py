from pathlib import Path

import pandas as pd
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

import logging

logger = logging.getLogger()


def read_json(spark: SparkSession, path: Path):
    logger.warning(msg = f"Reading JSON file from location: {path}")
    pdf = pd.read_json(path)
    return spark.createDataFrame(pdf)

def read_excel(spark: SparkSession, path: Path):
    logger.warning(msg = f"Reading Excel file from location: {path}")
    pdf = pd.read_excel(path)
    return spark.createDataFrame(pdf)

def read_csv(spark: SparkSession, path: Path, delimiter: str = ','):
    logger.warning(msg = f"Reading CSV file from location: {path}")    
    return spark.read.options(**{"header": "true", "delimiter": delimiter, "inferSchema": "true"}).csv(path)