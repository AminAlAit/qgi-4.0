"""TODO"""

import pyarrow as pa
import pyarrow.parquet as pq
import os
import logging


logger = logging.getLogger(__name__)


def load_runner(indicators, etl_input):
    logger.info("Starting data loading")
    try:
        config = etl_input["config"]
        for indicator in indicators:
            output_dir = os.path.join(config["output_path"], indicator.id)
            os.makedirs(output_dir, exist_ok=True)

            table = pa.Table.from_pandas(indicator.data)

            # Create a unique filename for each indicator
            output_file = os.path.join(output_dir, f"{indicator.id}.parquet")

            # Write the entire table to a single Parquet file
            pq.write_table(
                table,
                output_file,
                compression="snappy"
            )
        logger.info("Data loading completed successfully")
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        raise
