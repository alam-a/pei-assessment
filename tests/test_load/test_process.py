from pyspark.sql import DataFrame, SparkSession

from assessment.config.output_tables import OutputTables
from assessment.config.request_config import RequestConfig
from assessment.io.writer import save_dataframe_as_table
from assessment.load.process import process

def test_process(spark: SparkSession, test_request_config: RequestConfig, profit_aggregates_df: DataFrame):
    # setting up the test
    output_tables = OutputTables(test_request_config.db)
    save_dataframe_as_table(profit_aggregates_df, output_tables.PROFIT_AGGREGATES, test_request_config)
    
    # tables created by process shouldn't exist before run
    assert not spark.catalog.tableExists(output_tables.PROFIT_BY_CUSTOMER) 
    assert not spark.catalog.tableExists(output_tables.PROFIT_BY_YEAR) 
    assert not spark.catalog.tableExists(output_tables.PROFIT_BY_YEAR_CATEGORY) 
    assert not spark.catalog.tableExists(output_tables.PROFIT_BY_YEAR_CUSTOMER) 

    # running process
    process(spark=spark, request_config=test_request_config)

    # tables should exist now
    assert spark.catalog.tableExists(output_tables.PROFIT_BY_CUSTOMER) 
    assert spark.catalog.tableExists(output_tables.PROFIT_BY_YEAR) 
    assert spark.catalog.tableExists(output_tables.PROFIT_BY_YEAR_CATEGORY) 
    assert spark.catalog.tableExists(output_tables.PROFIT_BY_YEAR_CUSTOMER)

    assert spark.table(output_tables.PROFIT_BY_CUSTOMER).count() > 0 
    assert spark.table(output_tables.PROFIT_BY_YEAR).count() > 0 
    assert spark.table(output_tables.PROFIT_BY_YEAR_CATEGORY).count() > 0 
    assert spark.table(output_tables.PROFIT_BY_YEAR_CUSTOMER).count() > 0