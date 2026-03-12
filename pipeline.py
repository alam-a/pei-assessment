from pyspark.sql import SparkSession
from assessment.config.request_config import RequestConfig
from assessment.extract.process import process as process_extract
from assessment.transform.process import process as process_transform
from assessment.load.process import process as process_load

def run_pipeline(spark: SparkSession, request_config: RequestConfig):    
    try:
        # Load and normalize data
        process_extract(spark, request_config=request_config)
        process_transform(spark, request_config=request_config)
        process_load(spark, request_config=request_config)
        
    except Exception as e:
        print(f"Error in data pipeline: {e}")
        raise
    finally:
        spark.stop()
