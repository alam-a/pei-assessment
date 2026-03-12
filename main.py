from assessment.io.reader import logger
from assessment.utils.log_utils import setup_logging_config
from pipeline import run_pipeline
from assessment.config.request_config import RequestConfig
from assessment.config.initialize import init_database, init_spark

def main():
    setup_logging_config()
    logger.warning("Starting PEI Assessment Data Pipeline")
    logger.info(f"Database: {RequestConfig().db}")
    logger.info(f"Input location: {RequestConfig().input_location}")

    request_config = RequestConfig()
    spark = init_spark(request_config)
    init_database(spark, request_config)
    run_pipeline(spark, request_config)

if __name__ == "__main__":
    main()
