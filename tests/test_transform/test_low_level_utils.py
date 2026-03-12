import pytest
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
from pathlib import Path
from pyspark.sql.functions import col

phone_numbers_data = [
    ("123-456-w7890", "1234567890"),
    ("098-765[-4]321", "0987654321")
]

@pytest.fixture
def phone_numbers(tmp_path):
    file = tmp_path / "phone_numbers.json"
    data = pd.DataFrame(map(lambda x: {"phone": x[0]}, phone_numbers_data), columns=["phone"])
    data.to_json(file, orient="records")
    return str(file)

@pytest.fixture
def expected_cleaned_phone_numbers():
    return pd.DataFrame(map(lambda x: {"phone": x[1]}, phone_numbers_data), columns=["phone"])

@pytest.fixture
def path_and_primary_cols(tmp_path):
    # Create sample data with duplicates for testing
    sample_data = [
        {"id": 1, "name": "Salman Khan", "date": "21/8/2016", "value": 100},
        {"id": 2, "name": "Aamir Khan", "date": "23/9/2017", "value": 200},
        {"id": 1, "name": "Salman Khan", "date": "21/8/2016", "value": 100},  # duplicate
        {"id": 3, "name": "Aamir Kapoor.'123", "date": "15/10/2018", "value": 300}
    ]
    
    file = tmp_path / "sample_data.json"
    pd.DataFrame(sample_data).to_json(file, orient="records")
    return str(file), ["id"]

def test_clean_phone_numbers(spark: SparkSession, phone_numbers, expected_cleaned_phone_numbers):
    # Comparing with spark equality with 'from pyspark.testing.utils import assertDataFrameEqual'
    # is not working (system timezone issue) so we are comparing with pandas equality
    from assessment.transform.utils import clean_phone_numbers
    df: DataFrame = spark.read.json(phone_numbers)
    result = df.withColumn("phone", clean_phone_numbers("phone")).select("phone").toPandas()
    assert result.equals(expected_cleaned_phone_numbers)

def test_deduplicate_rows(spark: SparkSession, path_and_primary_cols):
    from assessment.transform.utils import deduplicate_rows
    
    file_path, primary_keys = path_and_primary_cols
    df: DataFrame = spark.read.json(file_path)
    
    original_count = df.count()
    deduplicated_df = deduplicate_rows(df, primary_keys)    
    deduplicated_count = deduplicated_df.count()
    
    assert deduplicated_count < original_count    
    assert deduplicated_count == 3
    
    primary_key_counts = deduplicated_df.groupBy(*primary_keys).count().filter(col("count") > 1)
    assert primary_key_counts.count() == 0

@pytest.mark.xfail(reason="Primary keys list cannot be empty")
def test_deduplicate_rows_with_empty_primary_keys(spark: SparkSession, path_and_primary_cols):
    from assessment.transform.utils import deduplicate_rows
    
    file_path, primary_keys = path_and_primary_cols
    df: DataFrame = spark.read.json(file_path)
    
    deduplicate_rows(df, [])

def test_deduplicate_using_aggregation(spark: SparkSession, path_and_primary_cols):
    from assessment.transform.utils import deduplicate_using_aggregation
    
    file_path, primary_keys = path_and_primary_cols
    df: DataFrame = spark.read.json(file_path)
    
    aggregation_rules = {"value": "sum", "name": "first"}
    deduplicated_df = deduplicate_using_aggregation(df, primary_keys, aggregation_rules)
    
    assert deduplicated_df.count() == 3
    
    primary_key_counts = deduplicated_df.groupBy(*primary_keys).count().filter(col("count") > 1)
    assert primary_key_counts.count() == 0
    
    id_1_row = deduplicated_df.filter(col("id") == 1).collect()[0]
    assert id_1_row["value"] == 200

@pytest.mark.xfail(reason="Primary keys list cannot be empty")
def test_deduplicate_using_aggregation_with_empty_primary_keys(spark: SparkSession, path_and_primary_cols):
    from assessment.transform.utils import deduplicate_using_aggregation
    
    file_path, primary_keys = path_and_primary_cols
    df: DataFrame = spark.read.json(file_path)
    deduplicate_using_aggregation(df, [])

@pytest.mark.xfail(reason="Primary keys list cannot be empty")
def test_deduplicate_using_aggregation_with_empty_aggregation_rules(spark: SparkSession, path_and_primary_cols):
    from assessment.transform.utils import deduplicate_using_aggregation
    
    file_path, primary_keys = path_and_primary_cols
    df: DataFrame = spark.read.json(file_path)
    deduplicate_using_aggregation(df, primary_keys)

    deduplicated_df = deduplicate_using_aggregation(df, primary_keys)
    
    assert deduplicated_df.count() == 3
    
    primary_key_counts = deduplicated_df.groupBy(*primary_keys).count().filter(col("count") > 1)
    assert primary_key_counts.count() == 0
    
    id_1_row = deduplicated_df.filter(col("id") == 1).collect()[0]
    assert id_1_row["value"] == 200

def test_convert_date_format(spark: SparkSession, path_and_primary_cols):
    from assessment.transform.utils import convert_date_str_to_date
    
    file_path, primary_keys = path_and_primary_cols
    df: DataFrame = spark.read.json(file_path)
    
    # Apply date format conversion
    df_with_converted_date = df.withColumn("date", convert_date_str_to_date(col("date"), "dd/M/yyyy"))
    
    # Get the converted dates
    converted_dates = df_with_converted_date.select("date").collect()
    
    # Check that dates are in the expected format
    import datetime
    assert isinstance(converted_dates[0]["date"], datetime.date)
    expected_dates = ["2016-08-21", "2017-09-23", "2016-08-21", "2018-10-15"]
    actual_dates = [str(row["date"]) for row in converted_dates]
    
    # Verify date conversion worked (dates should be in ISO format)
    for actual, expected in zip(actual_dates, expected_dates):
        assert actual == expected

def test_convert_name_to_alphabetic_with_name_case(spark: SparkSession, path_and_primary_cols):
    from assessment.transform.utils import convert_name_to_alphabetic_with_name_case
    
    file_path, primary_keys = path_and_primary_cols
    df: DataFrame = spark.read.json(file_path)
    
    # Apply name cleaning and case conversion
    df_with_cleaned_names = df.withColumn("name", convert_name_to_alphabetic_with_name_case(col("name")))
    
    # Get the cleaned names
    cleaned_names = df_with_cleaned_names.select("name").collect()
    
    # Expected names after cleaning and proper case conversion
    expected_names = ["Salman Khan", "Aamir Khan", "Salman Khan", "Aamir Kapoor"]
    actual_names = [row["name"] for row in cleaned_names]
    
    # Verify name cleaning worked
    for actual, expected in zip(actual_names, expected_names):
        assert actual == expected
    
    # Specifically check that "Aamir Kapoor.'123" became "Aamir Kapoor"
    aamir_row = df_with_cleaned_names.filter(col("id") == 3).collect()[0]
    assert aamir_row["name"] == "Aamir Kapoor"