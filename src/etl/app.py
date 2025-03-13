"""ETL Pipeline Application"""

import logging
from config import load_config  # pylint: disable=import-error
from etl_runner.index import etl  # pylint: disable=import-error


def main():
    """Main function to run the ETL pipeline"""
    try:
        # Load configuration
        config = load_config()

        # Prepare ETL input
        etl_input = {
            "indicator_ids": config["indicators"],
            "config": {
                "output_format": config["output_format"],
                "output_path": config["output_path"],
                "file_size_limit": config["file_size_limit"]
            }
        }

        # Execute ETL pipeline
        etl(etl_input)

        # Log success
        logging.info("ETL pipeline completed successfully")

    except Exception as e:
        # Log error and re-raise
        logging.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level = logging.INFO)
    main()
