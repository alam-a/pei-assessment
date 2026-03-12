from unittest import result
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
import pandas as pd
import pytest
from assessment.load.utils import calculate_profit_by_customer, calculate_profit_by_year, calculate_profit_by_year_category, calculate_profit_by_year_customer

def test_calculate_profit_by_year(profit_aggregates_df: DataFrame):
    result_df = calculate_profit_by_year(profit_aggregates_df).cache()

    assert result_df.count() == 3
    assert pytest.approx(result_df.filter(result_df['year'] == 2014).collect()[0]['total_profit']) == 13_185.81
    assert pytest.approx(result_df.filter(result_df['year'] == 2015).collect()[0]['total_profit']) == 19_130.86
    assert pytest.approx(result_df.filter(result_df['year'] == 2016).collect()[0]['total_profit']) ==  5_492.89

def test_calculate_profit_by_year_category(profit_aggregates_df: DataFrame):
    result_df = calculate_profit_by_year_category(profit_aggregates_df)

    assert result_df.count() == 6

    result_2014 = result_df.filter(result_df['year']== 2014)
    assert result_2014.count() == 2

    result_os_2014 = result_2014.filter(result_df['category'] == 'Office Supplies')
    assert result_os_2014.count() == 1

    assert pytest.approx(result_os_2014.collect()[0]['total_profit']) == 9_940.14
    
def test_calculate_profit_by_customer(profit_aggregates_df: DataFrame):
    result_df = calculate_profit_by_customer(profit_aggregates_df)

    assert result_df.count() == 6
    result_tamara = result_df.filter(result_df['customer_name'] == 'Tamara Chand')
    assert result_tamara.count() == 2 # two customers with the same name

    result_sanjit = result_df.filter(result_df['customer_name'] == 'Sanjit Engle')
    assert result_sanjit.count() == 1
    assert pytest.approx(result_sanjit.collect()[0]['total_profit']) == 6_491.34

def test_calculate_profit_by_year_customer(profit_aggregates_df: DataFrame):
    result_df = calculate_profit_by_year_customer(profit_aggregates_df)

    assert result_df.count() == 7
    assert pytest.approx(
        result_df.filter(
            (result_df['year'] == 2015) & (result_df['customer_name'] == 'Hunter Lopez')
            ).collect()[0]['total_profit']
        ) == 10_392.3
    assert pytest.approx(
        result_df.filter(
            (result_df['year'] == 2016) & (result_df['customer_name'] == 'Adrian Barton')
            ).collect()[0]['total_profit']
        ) == 5_492.89