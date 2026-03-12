import pytest
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from assessment.config.output_tables import OutputTables
from assessment.config.request_config import RequestConfig
from assessment.io.writer import save_dataframe_as_table
from assessment.transform.process import process
import pandas as pd
from tests.test_transform.sample_integration_data import orders, products, customers

def test_process(spark: SparkSession, test_request_config: RequestConfig):
    output_tables = OutputTables(test_request_config.db)
    
    # Generate test data
    save_dataframe_as_table(spark.createDataFrame(pd.DataFrame(orders)), output_tables.ORDER_EXTRACTS, test_request_config)
    save_dataframe_as_table(spark.createDataFrame(pd.DataFrame(products)), output_tables.PRODUCT_EXTRACTS, test_request_config)
    save_dataframe_as_table(spark.createDataFrame(pd.DataFrame(customers)), output_tables.CUSTOMER_EXTRACTS, test_request_config)
    process(spark, test_request_config)
    
    # Verify transformed data
    assert spark.read.table(output_tables.ORDER_TRANSFORMED).count() > 0
    assert spark.read.table(output_tables.PRODUCT_TRANSFORMED).count() > 0
    assert spark.read.table(output_tables.CUSTOMER_TRANSFORMED).count() > 0
    assert spark.read.table(output_tables.PRODUCT_TRANSFORMED).count() < spark.read.table(output_tables.PRODUCT_EXTRACTS).count()

    assert spark.read.table(output_tables.ENRICHED_PRODUCTS).count() > 0
    assert spark.read.table(output_tables.ENRICHED_CUSTOMERS).count() > 0
    assert spark.read.table(output_tables.ENRICHED_ORDERS).count() > 0
    assert spark.read.table(output_tables.PROFIT_AGGREGATES).count() > 0
