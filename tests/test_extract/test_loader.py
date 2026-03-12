from unittest.mock import MagicMock, patch
import pytest
from pyspark.sql import SparkSession
import pandas as pd
import os
from pathlib import Path
from assessment.config.output_tables import OutputTables
from assessment.config.request_config import RequestConfig
from assessment.io.reader import read_csv
from assessment.extract.process import normalize_column_names, process

CSV_FILE_NAME = 'Products.csv'
EXCEL_FILE_NAME = 'Customer.xlsx'
JSON_FILE_NAME = 'Orders.json'

@pytest.fixture
def sample_csv_path(tmp_path, sample_uncleaned_headers):
    file = tmp_path / "sample.csv"
    pd.DataFrame(sample_uncleaned_headers).to_csv(file)
    return str(file)

def test_normalize_column_names(spark: SparkSession, sample_csv_path):
    df = read_csv(spark, sample_csv_path)
    normalized_df = normalize_column_names(df)
    assert normalized_df.count() == 1
    assert "first Name" not in normalized_df.columns
    assert "first_name" in normalized_df.columns
    assert "second_name" in normalized_df.columns

@pytest.fixture
def base_path(sample_data, tmp_path):
    # Generate CSV
    csv_path = tmp_path / CSV_FILE_NAME
    pd.DataFrame(sample_data).to_csv(csv_path)

    #Generate JSON
    json_path = tmp_path / JSON_FILE_NAME
    pd.DataFrame(sample_data).to_json(json_path, orient='records')
    
    #Generate Excel
    excel_path = tmp_path / EXCEL_FILE_NAME
    pd.DataFrame(sample_data).to_excel(excel_path)

    
    return tmp_path

def test_process(spark: SparkSession, test_request_config, base_path):
    test_request_config.input_location = base_path
    output_tables = OutputTables(test_request_config.db)

    assert not spark.catalog.tableExists(output_tables.PRODUCT_EXTRACTS)
    assert not spark.catalog.tableExists(output_tables.CUSTOMER_EXTRACTS)
    assert not spark.catalog.tableExists(output_tables.ORDER_EXTRACTS)
    
    process(spark, test_request_config)
    
    assert spark.catalog.tableExists(output_tables.PRODUCT_EXTRACTS)
    assert spark.catalog.tableExists(output_tables.CUSTOMER_EXTRACTS)
    assert spark.catalog.tableExists(output_tables.ORDER_EXTRACTS)
