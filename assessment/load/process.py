from pyspark.sql import SparkSession

from assessment.config.request_config import RequestConfig
from assessment.config.output_tables import OutputTables
from assessment.io.writer import save_dataframe_as_table
from assessment.load.utils import calculate_profit_by_year, calculate_profit_by_year_category, calculate_profit_by_customer, calculate_profit_by_year_customer


def process(spark: SparkSession, request_config: RequestConfig) -> None:
    """Calculate and save aggregates to show in the dashboard"""
    output_tables = OutputTables(request_config.db)
    profit_aggregates = spark.table(output_tables.PROFIT_AGGREGATES).cache()

    profit_by_year = calculate_profit_by_year(profit_aggregates)
    save_dataframe_as_table(profit_by_year, output_tables.PROFIT_BY_YEAR, request_config)

    profit_by_year_category = calculate_profit_by_year_category(profit_aggregates)
    save_dataframe_as_table(profit_by_year_category, output_tables.PROFIT_BY_YEAR_CATEGORY, request_config)

    # Keeping both id and name for customer as many customers may share the same name
    profit_by_customer = calculate_profit_by_customer(profit_aggregates)
    save_dataframe_as_table(profit_by_customer, output_tables.PROFIT_BY_CUSTOMER, request_config)

    profit_by_year_customer = calculate_profit_by_year_customer(profit_aggregates)
    save_dataframe_as_table(profit_by_year_customer, output_tables.PROFIT_BY_YEAR_CUSTOMER, request_config)
