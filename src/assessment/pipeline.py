from typing import List, Optional
from pyspark.sql import SparkSession
from assessment.io.reader import logger
from assessment.utils.log_utils import setup_logging_config
from assessment.config.request_config import RequestConfig
from assessment.config.initialize import init_database, init_spark

def run(args: Optional[List[str]] = None):
    try:
        print("Setting up the process")
        setup_logging_config()

        logger.warning("Starting PEI Assessment Data Pipeline")
        request_config = RequestConfig(args)
        logger.info(f"Database: {request_config.db}")
        logger.info(f"Input location: {request_config.input_location}")
        spark: SparkSession = init_spark(request_config)
        init_database(spark, request_config)

        try:
            from assessment.extract.process import process as process_extract
            from assessment.transform.process import process as process_transform
            from assessment.load.process import process as process_load

            logger.warning("Starting pipeline processing")
            process_extract(spark, request_config=request_config)
            process_transform(spark, request_config=request_config)
            process_load(spark, request_config=request_config)
            logger.warning("Finished processing pipeline")
        except Exception as e:
            logger.error(f"Processing failed with error: {e}")
        
    except Exception as e:
        print(f"Setup failed for data pipeline with error: {e}")
        raise