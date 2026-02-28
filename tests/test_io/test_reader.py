import pytest
from pyspark.sql import SparkSession
import pandas as pd
from pathlib import Path
from assessment.io.reader import read_json, read_excel, read_csv

@pytest.fixture
def sample_json_path(tmp_path, sample_data):
    file = tmp_path / "orders.json"
    pd.DataFrame(sample_data).to_json(file)
    # file.write_text("""
    #                 [{"first_name": "Salman", "second_name": "Khan", "age": 55}]
    #                 """)
    return str(file)

@pytest.fixture
def corrupted_json_path(tmp_path):
    file = tmp_path / "corrupted.json"
    file.write_text("""
        hello
    """)


@pytest.fixture
def sample_excel_path(tmp_path, sample_data):
    file = tmp_path / "excel.xlsx"
    pd.DataFrame(sample_data).to_excel(file)
    return str(file)

@pytest.fixture
def sample_csv_path(tmp_path, sample_data):
    file = tmp_path / "sample.csv"
    pd.DataFrame(sample_data).to_csv(file)
    return str(file)

def test_read_json(spark: SparkSession, sample_json_path):
    df = read_json(spark, sample_json_path)

    assert df.count() == 1
    assert "first_name" in df.columns
    assert "second_name" in df.columns
    assert "third_name" not in df.columns
    assert "age" in df.columns
    assert df.filter("age = 50").count() == 0

@pytest.mark.xfail(reason="Reading corrupted JSON will lead to failure")
def test_read_json_failure(spark: SparkSession, corrupted_json_path):
    read_json(spark, corrupted_json_path)



def test_read_excel(spark: SparkSession, sample_excel_path):
    df = read_excel(spark, sample_excel_path)
    assert df.count() == 1
    assert "first_name" in df.columns
    assert "second_name" in df.columns
    assert "third_name" not in df.columns
    assert "age" in df.columns
    assert df.filter("age = 50").count() == 0



def test_read_csv(spark: SparkSession, sample_csv_path: Path):
    df = read_csv(spark, sample_csv_path, delimiter=',')
    assert df.count() == 1
    assert "first_name" in df.columns
    assert "second_name" in df.columns
    assert "third_name" not in df.columns
    assert "age" in df.columns
    assert df.filter("age = 50").count() == 0

@pytest.mark.xfail(reason="Using wrong delimiter will cause all values to form a single column")
def test_read_csv_wrong_delimiter(spark: SparkSession, sample_csv_path: Path):
    df = read_csv(spark, sample_csv_path, delimiter='|')
    assert df.count() == 1
    assert "first_name" in df.columns
    assert "second_name" in df.columns
    assert "third_name" not in df.columns
    assert "age" in df.columns

def test_read_csv_wrong_delimiter_access_column(spark: SparkSession, sample_csv_path: Path):
    df = read_csv(spark, sample_csv_path, delimiter='|')
    from pyspark.errors.exceptions.captured import AnalysisException
    with pytest.raises(AnalysisException):
        df.filter("age = 50").count() == 0
