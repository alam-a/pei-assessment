from pyspark.sql import SparkSession
from assessment.config.request_config import RequestConfig
from assessment.extract.load_data_sources import load
from assessment.transform.process import process
from assessment.transform.utils import create_enriched_customers_products_table, create_enriched_orders_table
from assessment.load.aggregates import create_aggregate_table, show_aggregates_in_sql

def run_pipeline(spark: SparkSession, request_config: RequestConfig):    
    # base_path = "/Users/alaf/dev/pei-assessment/data"
    base_path = request_config.input_location

    try:
        # Load and normalize data
        load(spark, request_config)
        process(spark, base_path)
        create_enriched_customers_products_table(spark, base_path)
        create_enriched_orders_table(spark, base_path)
        create_aggregate_table(spark, base_path)
        show_aggregates_in_sql(spark, base_path)
        
    except Exception as e:
        print(f"Error in data pipeline: {e}")
        raise
    finally:
        spark.stop()
