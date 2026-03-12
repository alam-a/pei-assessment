from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

def calculate_profit_by_year(profit_aggregates: DataFrame) -> DataFrame:
    profit_by_year = (
        profit_aggregates
        .groupBy("year")
        .agg(F.round(F.sum("total_profit"), 2).alias("total_profit"))
    )
    return profit_by_year


def calculate_profit_by_year_category(profit_aggregates: DataFrame) -> DataFrame:
    profit_by_year_category = (
        profit_aggregates
        .groupBy("year", "category")
        .agg(F.round(F.sum("total_profit"), 2).alias("total_profit"))
    )
    return profit_by_year_category


def calculate_profit_by_customer(profit_aggregates: DataFrame) -> DataFrame:
    # Keeping both id and name for customer as many customers may share the same name
    profit_by_customer = (
        profit_aggregates
        .groupBy("customer_id", "customer_name")
        .agg(F.round(F.sum("total_profit"), 2).alias("total_profit"))
    )
    return profit_by_customer

def calculate_profit_by_year_customer(profit_aggregates: DataFrame) -> DataFrame:
    profit_by_year_customer = (
        profit_aggregates
        .groupBy("year", "customer_id", "customer_name")
        .agg(F.round(F.sum("total_profit"), 2).alias("total_profit"))
    )
    return profit_by_year_customer
