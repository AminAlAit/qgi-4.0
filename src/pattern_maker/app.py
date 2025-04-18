"""TODO"""

import logging
import os
import glob
import shutil
import pandas as pd
import dask.dataframe as dd


TOP_PPS_PATTERNS_COUNT = 100_000


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()


class PatternMaker:
    """TODO"""

    def __init__(self, scdi_finder_output: str, pattern_maker_output: str, countries_csv: str):
        self.scdi_finder_output = scdi_finder_output
        self.pattern_maker_output = pattern_maker_output
        self.countries_csv = countries_csv

    def run(self):
        """Run the entire pattern creation and processing pipeline"""
        try:
            # Step 1: Create patterns and save to Parquet
            logger.info("Starting pattern creation...")
            self.create_patterns()
            logger.info("Pattern creation completed successfully")

            # Step 2: Filter out patterns with scdi_count = 1
            logger.info("Filtering out patterns with scdi_count = 1...")
            self.filter_scdi_count()
            logger.info("Filtering completed successfully")

            # logger.info("Starting creation of symmetric patterns...")
            # self.create_symmetric_patterns()
            # logger.info("Symmetric pattern creation completed successfully")

            logger.info("Removing duplicate patterns...")
            self.remove_duplicate_patterns()
            logger.info("Duplicate removal completed successfully")

            # Step 3: Add PPS to each Parquet file
            logger.info("Starting PPS calculation...")
            self.add_pps()
            logger.info("PPS calculation completed successfully")

            # Step 4: Normalize PPS across all Parquets
            logger.info("Starting PPS normalization...")
            self.normalize_pps()
            logger.info("PPS normalization completed successfully")

            # Step 5: Create 'top_pps.parquet' with top patterns
            logger.info("Starting creation of 'top_pps.parquet'...")
            self.create_top_pps()
            logger.info("'top_pps.parquet' creation completed successfully")

            logger.info("Entire pipeline executed successfully")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise

    def _find_scdi_parquet_files(self):
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

        scdi_parquet_files = self._find_scdi_parquet_files()

        logger.info("Creating a Dask DataFrame from all Parquet files...")
        ddf = dd.read_parquet(scdi_parquet_files)

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

        patterns_ddf.columns = [
            'country_a_id',
            'country_b_id',
            'length',
            'start_year_a',
            'start_year_b',
            'average_correlation',
            'scdi_count'
        ]

        # Save the patterns to Parquet files
        logger.info(f"Saving patterns to {self.pattern_maker_output}...")
        patterns_ddf.to_parquet(
            self.pattern_maker_output,
            compression="brotli",
            write_index=False
        )

        logger.info(f"Created and saved the 'patterns' table successfully")

    def filter_scdi_count(self):
        """Filter out patterns with scdi_count = 1"""

        logger.info("Filtering out patterns with scdi_count = 1...")
        patterns_files = glob.glob(os.path.join(self.pattern_maker_output, "*.parquet"))
        if not patterns_files:
            logger.warning("No pattern files found. Skipping filtering...")
            return

        for file in patterns_files:
            logger.info(f"Processing file: {file}")
            df = pd.read_parquet(file)

            # Filter out rows where scdi_count == 1
            df = df[df['scdi_count'] != 1]

            # Save the updated DataFrame back to Parquet
            df.to_parquet(
                file,
                index=False,
                compression="brotli"
            )
            logger.info(f"Saved filtered Parquet file: {file}")

    def create_symmetric_patterns(self):
        """Create symmetric patterns by swapping country_a_id with country_b_id and start_year_a with start_year_b"""
        logger.info("Creating symmetric patterns by swapping country_a_id with country_b_id and start_year_a with start_year_b...")

        scdi_parquet_files = self._find_scdi_parquet_files()

        logger.info("Creating a Dask DataFrame from all Parquet files...")
        ddf = dd.read_parquet(scdi_parquet_files)

        # Swap country_a_id with country_b_id and start_year_a with start_year_b
        logger.info("Swapping country_a_id with country_b_id and start_year_a with start_year_b...")
        ddf_swapped = ddf.rename(columns={
            'country_a_id': 'country_b_id',
            'country_b_id': 'country_a_id',
            'start_year_a': 'start_year_b',
            'start_year_b': 'start_year_a'
        })

        # Group by the new columns and aggregate
        logger.info("Grouping and aggregating data for symmetric patterns...")
        symmetric_patterns_ddf = ddf_swapped.groupby([
            'country_a_id',
            'country_b_id',
            'length',
            'start_year_a',
            'start_year_b'
        ]).agg({
            'correlation': ['mean', 'count']
        }).reset_index()

        symmetric_patterns_ddf.columns = [
            'country_a_id',
            'country_b_id',
            'length',
            'start_year_a',
            'start_year_b',
            'average_correlation',
            'scdi_count'
        ]

        # Combine with original patterns
        logger.info("Combining symmetric patterns with original patterns...")
        original_patterns_ddf = dd.read_parquet(os.path.join(self.pattern_maker_output, "*.parquet"))
        combined_patterns_ddf = dd.concat([original_patterns_ddf, symmetric_patterns_ddf])

        # Save the combined patterns to Parquet files
        logger.info(f"Saving combined patterns to {self.pattern_maker_output}...")
        combined_patterns_ddf.to_parquet(
            self.pattern_maker_output,
            index=False,
            compression="brotli",
            write_index=False
        )

        logger.info("Combined patterns saved successfully")

    def remove_duplicate_patterns(self):
        """Remove duplicate patterns across all Parquet files"""
        logger.info("Removing duplicate patterns across all Parquet files...")
        patterns_files = glob.glob(os.path.join(self.pattern_maker_output, "*.parquet"))
        if not patterns_files:
            logger.warning("No pattern files found. Skipping duplicate removal...")
            return

        # Read all Parquet files into a Dask DataFrame
        logger.info("Reading all Parquet files into a Dask DataFrame...")
        ddf = dd.read_parquet(patterns_files)

        # Remove duplicates based on the specified columns
        logger.info("Removing duplicates...")
        unique_patterns_ddf = ddf.drop_duplicates(
            subset=['country_a_id', 'country_b_id', 'length', 'start_year_a', 'start_year_b']
        )

        # Save the unique patterns to a temporary directory
        temp_dir = os.path.join(self.pattern_maker_output, "temp")
        os.makedirs(temp_dir, exist_ok=True)
        logger.info(f"Saving unique patterns to temporary directory: {temp_dir}...")
        unique_patterns_ddf.to_parquet(
            temp_dir,
            compression="brotli",
            write_index=False
        )

        # Replace original files with the unique patterns
        logger.info("Replacing original files with unique patterns...")
        # Remove original Parquet files
        for file in patterns_files:
            os.remove(file)
            logger.debug(f"Removed original file: {file}")

        # Move files from temporary directory to the original directory
        temp_parquet_files = glob.glob(os.path.join(temp_dir, "*.parquet"))
        for file in temp_parquet_files:
            shutil.move(file, self.pattern_maker_output)
            logger.debug(f"Moved file to {self.pattern_maker_output}: {file}")

        # Remove the temporary directory
        shutil.rmtree(temp_dir)
        logger.info("Removed temporary directory: {temp_dir}")

        logger.info("Duplicate removal completed successfully")

    def add_pps(self):
        """Add Pattern Power Score (PPS) to each Parquet file"""

        logger.info("Calculating Pattern Power Score (PPS) for each Parquet file...")
        patterns_files = glob.glob(os.path.join(self.pattern_maker_output, "*.parquet"))
        if not patterns_files:
            logger.warning("No pattern files found. Skipping PPS calculation...")
            return

        for file in patterns_files:
            logger.info(f"Processing file: {file}")
            df = pd.read_parquet(file)

            # Calculate PPS
            df['pps'] = (
                0.33 * df['length'] +
                0.33 * df['average_correlation'] +
                0.34 * df['scdi_count']
            )

            # Save the updated DataFrame back to Parquet
            df.to_parquet(
                file,
                compression="brotli"
            )
            logger.info(f"Saved updated Parquet file with PPS: {file}")

    def normalize_pps(self):
        """Normalize PPS values across all Parquet files"""

        logger.info("Normalizing PPS values across all Parquet files...")
        patterns_files = glob.glob(os.path.join(self.pattern_maker_output, "*.parquet"))
        if not patterns_files:
            logger.warning("No pattern files found. Skipping PPS normalization...")
            return

        # Read all Parquet files into a Dask DataFrame
        logger.info("Reading all Parquet files into a Dask DataFrame...")
        ddf = dd.read_parquet(patterns_files)

        # Calculate global min and max PPS
        logger.info("Calculating global min and max PPS...")
        pps_min = ddf['pps'].min().compute()
        pps_max = ddf['pps'].max().compute()

        logger.info(f"Global PPS min: {pps_min}, max: {pps_max}")

        # Normalize PPS in each Parquet file
        for file in patterns_files:
            logger.info(f"Normalizing PPS in file: {file}")
            df = pd.read_parquet(file)

            # Normalize PPS
            df['pps_normalized'] = ((df['pps'] - pps_min) / (pps_max - pps_min)) * 100

            # Save the updated DataFrame back to Parquet
            df.to_parquet(
                file,
                compression="brotli"
            )
            logger.info(f"Saved normalized Parquet file: {file}")

    def create_top_pps(self):
        """Create a Parquet file with top N patterns based on PPS"""

        logger.info(f"Creating 'top_pps.parquet' with top {TOP_PPS_PATTERNS_COUNT} patterns based on PPS...")
        patterns_files = glob.glob(os.path.join(self.pattern_maker_output, "*.parquet"))
        if not patterns_files:
            logger.warning("No pattern files found. Skipping creation of 'top_pps.parquet'...")
            return

        # Read all Parquet files into a Dask DataFrame
        logger.info("Reading all Parquet files into a Dask DataFrame...")
        ddf = dd.read_parquet(patterns_files)

        # Ensure ddf is a Dask DataFrame
        if not isinstance(ddf, dd.DataFrame):
            logger.error("ddf is not a Dask DataFrame. Cannot proceed.")
            return

        # Sort by PPS and select top N patterns
        logger.info(f"Selecting top {TOP_PPS_PATTERNS_COUNT} patterns based on PPS...")
        top_patterns = ddf.sort_values('pps', ascending=False).head(TOP_PPS_PATTERNS_COUNT).compute()

        # Save top patterns to a Parquet file
        output_path = os.path.join(self.pattern_maker_output, "top_pps.parquet")
        top_patterns.to_parquet(
            output_path,
            index=False,
            compression="brotli"
        )
        logger.info(f"Saved 'top_pps.parquet' to {output_path}")

# TODO: replace/add country names?
creator = PatternMaker(
    scdi_finder_output   = r"/workspace/qgi\qgi-4.0\data\scdi_finder_output",
    pattern_maker_output = r"/workspace/qgi\qgi-4.0\data\pattern_maker_output",
    countries_csv        = r"/workspace/qgi\qgi-4.0\data\countries.csv"
)
creator.run()
