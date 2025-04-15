"""TODO"""

import logging
import os
import glob
import shutil
import pandas as pd
import dask.dataframe as dd


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()


class PatternMaker:
    """TODO"""

    def __init__(self, scdi_finder_output: str, pattern_maker_output: str, countries_csv: str):
        """Initialize the query manager with the base folder path"""

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

            # Step 2: Process country subfolders, merge Parquets, filter, reorder, and save
            logger.info("Starting processing of country subfolders...")
            self.process_country_subfolders()
            logger.info("Processing of country subfolders completed successfully")

            # Step 3: Rename Parquet files to country names
            logger.info("Starting renaming of Parquet files to country names...")
            self.rename_parquets_by_country_name()
            logger.info("Renaming of Parquet files completed successfully")

            logger.info("Entire pipeline executed successfully")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise

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

        logger.info("Creating a Dask DataFrame from all Parquet files...")
        ddf = dd.read_parquet(parquet_files)

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

        # Partition by 'country_a_id' and save the patterns to Parquet files
        logger.info(f"Saving patterns to {self.pattern_maker_output}...")
        patterns_ddf.to_parquet(
            path=self.pattern_maker_output,
            compression="brotli",
            write_index=False,
            partition_on=['country_a_id'],
            name_function=lambda i: f"part.{i}.parquet"
        )

        logger.info(f"Created and saved the 'patterns' table successfully")

    def process_country_subfolders(self):
        """
        Process each country_a_id subfolder, merge Parquets, filter, reorder, and save.
        """
        # Find all country_a_id subfolders
        subfolders = glob.glob(os.path.join(self.pattern_maker_output, "country_a_id=*"))
        filtered_out_scdis = 0

        for subfolder in subfolders:
            # Extract country_a_id from the subfolder name
            country_a_id = os.path.basename(subfolder).split('=')[1]
            logger.info(f"Processing subfolder: {subfolder} (country_a_id={country_a_id})")

            # Find all Parquet files in the subfolder
            parquet_files = glob.glob(os.path.join(subfolder, "*.parquet"))
            if not parquet_files:
                logger.warning(f"No Parquet files found in {subfolder}. Skipping...")
                continue

            # Read and merge all Parquet files
            dfs = []
            for file in parquet_files:
                df = pd.read_parquet(file)
                dfs.append(df)

            if not dfs:
                logger.warning(f"No data found in {subfolder}. Skipping...")
                continue

            merged_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Merged {len(parquet_files)} Parquet files into a DataFrame with {len(merged_df)} rows")

            # Filter rows where scdi_count == 1
            filtered_df = merged_df[merged_df['scdi_count'] != 1]
            logger.info(f"Filtered out {len(merged_df) - len(filtered_df)} rows with scdi_count == 1")
            filtered_out_scdis += len(merged_df) - len(filtered_df)

            # Reorder columns
            column_order = [
                "country_a_id",
                "country_b_id",
                "length",
                "start_year_a",
                "start_year_b",
                "average_correlation",
                "scdi_count"
            ]
            filtered_df = filtered_df.reindex(columns=column_order)
            logger.info("Reordered columns successfully")

            # Save the merged and filtered DataFrame to a new Parquet file
            output_filename = f"country_a_id={country_a_id}.parquet"
            output_path = os.path.join(self.pattern_maker_output, output_filename)
            filtered_df.to_parquet(
                output_path,
                index=False,
                compression="brotli"
            )
            logger.info(f"Saved merged and filtered data to {output_path}")

            # Move the new Parquet file to the parent directory (if needed)
            # and delete the original subfolder
            try:
                # Remove the original subfolder
                shutil.rmtree(subfolder)
                logger.info(f"Removed original subfolder: {subfolder}")
            except Exception as e:
                logger.error(f"Failed to remove subfolder {subfolder}: {str(e)}")

        logger.info(f"Filtered out a total of {filtered_out_scdis} patterns with single SCDIs.")

    def rename_parquets_by_country_name(self):
        """
        Rename Parquet files from "country_a_id=<id>.parquet" to the country's real name.
        """
        # Read the countries CSV file to create a mapping from id to name_1
        logger.info(f"Reading countries CSV file: {self.countries_csv}")
        countries_df = pd.read_csv(self.countries_csv)
        id_to_name = dict(zip(countries_df['id'], countries_df['name_1']))

        # Find all Parquet files in the base directory that match the pattern "country_a_id=*.parquet"
        parquet_files = glob.glob(os.path.join(self.pattern_maker_output, "country_a_id=*.parquet"))

        for file_path in parquet_files:
            # Extract the country_a_id from the filename
            filename = os.path.basename(file_path)
            country_id_str = filename.split('=')[1].split('.')[0]
            try:
                country_id = int(country_id_str)
            except ValueError:
                logger.warning(f"Could not extract country ID from filename: {filename}")
                continue

            # Get the corresponding country name
            country_name = id_to_name.get(country_id)
            if not country_name:
                logger.warning(f"No country name found for ID {country_id}")
                continue

            # Create the new filename
            new_filename = f"{country_name}.parquet"
            new_file_path = os.path.join(self.pattern_maker_output, new_filename)

            # Rename the file
            try:
                shutil.move(file_path, new_file_path)
                logger.info(f"Renamed {filename} to {new_filename}")
            except Exception as e:
                logger.error(f"Failed to rename {filename}: {str(e)}")


pattern_maker_output_path = r"/workspace/qgi\qgi-4.0\data\pattern_maker_output"
scdi_finder_output_path = r"/workspace/qgi\qgi-4.0\data\scdi_finder_output"
countries_csv_path = r"/workspace/qgi\qgi-4.0\data\countries.csv"

creator = PatternMaker(
    scdi_finder_output=scdi_finder_output_path,
    pattern_maker_output=pattern_maker_output_path,
    countries_csv=countries_csv_path
)
creator.run()
