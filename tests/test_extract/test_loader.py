import pytest
from pyspark.sql import SparkSession
import pandas as pd
import os
from pathlib import Path
from assessment.io.reader import read_csv
from assessment.extract.load_data_sources import normalize_column_names, load, flatten_json_df

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

def test_load(spark: SparkSession, base_path):
    print(os.listdir(base_path))
    assert not os.path.exists(base_path / 'outputs' / 'orders.parquet')
    assert not os.path.exists(base_path / 'outputs' / 'products.parquet')
    assert not os.path.exists(base_path / 'outputs' / 'customers.parquet')
    
    load(spark, base_path)
    
    assert os.path.exists(base_path / 'outputs' / 'products.parquet')
    assert os.path.exists(base_path / 'outputs' / 'customers.parquet')
    assert os.path.exists(base_path / 'outputs' / 'orders.parquet')
