import logging
from logging import getLogger

def setup_logging_config():
    logging.basicConfig(
        filename='logs.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a' 
    )   