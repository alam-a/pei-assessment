from pathlib import Path

import pandas as pd
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

import logging

logger = logging.getLogger()


def read_json(spark: SparkSession, path: Path):
    logger.warning(msg = f"Reading JSON file from location: {path}")
    options = {
        "inferSchema": "true", 
        "multiLine": "true", 
        "mode": "PERMISSIVE", 
        "columnNameOfCorruptRecord": "_corrupt_record"
    }
    return spark.read.options(**options).json(path)


def read_excel(spark: SparkSession, path: Path):
    logger.warning(msg = f"Reading Excel file from location: {path}")
    # In Production, we would do something like this, but this needed extra setup in local, so skipped for now
    # return spark.read.format("com.crealytics.spark.excel").option("header", "true").load(path)
    return pd.read_excel(path)

def read_csv(spark: SparkSession, path: Path, delimiter: str = ','):
    logger.warning(msg = f"Reading CSV file from location: {path}") 
    options = {
        "header": "true", 
        "delimiter": delimiter, 
        "inferSchema": "true", 
        "multiLine": "true", 
        "quote": '"', 
        "escape": '"', 
        "mode": "PERMISSIVE", 
        "columnNameOfCorruptRecord": "_corrupt_record"
    }   
    return spark.read\
        .options(**options).csv(path)
