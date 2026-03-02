from assessment.utils.log_utils import setup_logging_config
from pipeline import run_pipeline

def main():
    setup_logging_config()
    run_pipeline()

if __name__ == "__main__":
    main()
