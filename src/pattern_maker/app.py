"""TODO"""

import logging
import os
import dask.dataframe as dd


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()


class SDCIPatternCreator:
    """TODO"""

    def __init__(self, scdi_finder_output: str, pattern_maker_output: str):
        """Initialize the query manager with the base folder path"""
        self.scdi_finder_output = scdi_finder_output
        self.pattern_maker_output = pattern_maker_output

    def _find_parquet_files(self):
        """Find all Parquet files in the directory structure"""
        parquet_files = []
        logger.info("Searching for Parquet files...")
        for root, dirs, files in os.walk(self.scdi_finder_output):
            # Skip directories ending with "_shrinked"
            if root.endswith("_shrinked"):
                continue

            for file in files:
                if file.endswith('.parquet'):
                    full_path = os.path.join(root, file)
                    parquet_files.append(full_path)
                    logger.debug(f"Found Parquet file: {full_path}")

        if not parquet_files:
            raise ValueError("No Parquet files found in the specified directory structure")

        logger.info(f"Found {len(parquet_files)} Parquet files")
        return parquet_files

    def create_patterns(self):
        """Create patterns by grouping SCDIs and save to Parquet"""
        logger.info("Creating patterns by grouping SCDIs...")

        parquet_files = self._find_parquet_files()

        # Create a Dask DataFrame from all Parquet files
        logger.info("Reading Parquet files with Dask...")
        ddf = dd.read_parquet(parquet_files)

        # Group by the specified columns and calculate average correlation and count
        logger.info("Grouping and aggregating data...")
        patterns_ddf = ddf.groupby([
            'country_a_id',
            'country_b_id',
            'length',
            'start_year_a',
            'start_year_b'
        ]).agg({
            'correlation': ['mean', 'count']
        }).reset_index()

        # Rename columns
        patterns_ddf.columns = [
            'country_a_id',
            'country_b_id',
            'length',
            'start_year_a',
            'start_year_b',
            'average_correlation',
            'scdi_count'
        ]

        # Save the patterns to Parquet files organized by country_a_id
        logger.info(f"Saving patterns to {self.pattern_maker_output}...")
        patterns_ddf.to_parquet(
            path=self.pattern_maker_output,
            compression="brotli",
            #overwrite=True, # to overwrite existing patterns parquet file
            write_index=False
        )

        logger.info(f"Created and saved the 'patterns' table successfully")

# Usage
pattern_maker_output_path = r"/workspace/qgi\qgi-4.0\data\pattern_maker_output"
scdi_finder_output_path = r"/workspace/qgi\qgi-4.0\data\scdi_finder_output"

creator = SDCIPatternCreator(scdi_finder_output=scdi_finder_output_path, pattern_maker_output=pattern_maker_output_path)
creator.create_patterns()
