"""TODO"""

import logging
import os
import glob
import shutil
import pandas as pd
import dask.dataframe as dd
import numpy as np
from collections import defaultdict


TOP_PPS_PATTERNS_COUNT = 100_000


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()


class SymmetricPatternGenerator:
    """
    A class used to generate symmetric patterns from existing pattern data.
    
    This class processes existing patterns to create symmetric counterparts by swapping country_a_id with country_b_id and start_year_a with start_year_b.
    It ensures relationships between countries are bidirectional and aggregates symmetric patterns with existing patterns.
    
    Attributes:
        pattern_maker_output (str): Path to the directory where pattern data is stored.
        countries_csv (str): Path to the CSV file containing country mappings.
        logger (logging.Logger): Logger instance for logging events.
        checkpoint_file (str): Path to the checkpoint file for tracking progress.
    """
    
    def __init__(self, pattern_maker_output, countries_csv, checkpoint_file = "checkpoint.txt"):
        """
        Initialize the SymmetricPatternGenerator with necessary file paths.
        
        Args:
            pattern_maker_output (str): Path to the directory where pattern data is stored.
            countries_csv (str): Path to the CSV file containing country mappings.
            checkpoint_file (str): Path to the checkpoint file for tracking progress.
        """
        self.pattern_maker_output = pattern_maker_output
        self.countries_csv = countries_csv
        self.checkpoint_file = checkpoint_file
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
    
    def create_symmetric_patterns(self):
        """
        Create symmetric patterns by swapping country_a_id with country_b_id and start_year_a with start_year_b.
        
        This method processes existing patterns to create their symmetric counterparts, 
        ensuring that relationships between countries are bidirectional. The symmetric patterns 
        are then aggregated with existing patterns to provide a comprehensive view of 
        country relationships.
        """
        self.logger.info("Creating symmetric patterns...")
        
        # Load checkpoint
        processed_countries = self._load_checkpoint()
        
        # Get list of pattern directories
        pattern_dirs = self._get_pattern_directories()
        
        # Preload country mappings
        country_id_to_name = self._load_country_mappings()
        
        # Process each country directory
        for country_dir in pattern_dirs:
            country_name = os.path.basename(country_dir)
            
            # Skip already processed countries
            if country_name in processed_countries:
                self.logger.info(f"Skipping already processed country: {country_name}")
                continue
            
            try:
                self._process_country_directory(country_dir, country_id_to_name)
                # Update checkpoint
                processed_countries.add(country_name)
                self._save_checkpoint(processed_countries)
            except Exception as e:
                self.logger.error(f"Error processing country {country_name}: {str(e)}")
                # Optionally re-raise the exception if you want the program to stop
                # raise
            
        self.logger.info("Symmetric pattern creation completed successfully")
        
        # Reset checkpoint if all countries are processed
        if len(processed_countries) == len(pattern_dirs):
            self.logger.info("All countries processed. Resetting checkpoint.")
            self._save_checkpoint(set())

    def _load_checkpoint(self):
        """Load the set of processed countries from the checkpoint file
        
        Returns:
            set: Set of country names that have been processed
        """
        if not os.path.exists(self.checkpoint_file):
            return set()
        
        with open(self.checkpoint_file, 'r') as f:
            return set(f.read().splitlines())
    
    def _save_checkpoint(self, processed_countries):
        """Save the set of processed countries to the checkpoint file
        
        Args:
            processed_countries (set): Set of country names that have been processed
        """
        with open(self.checkpoint_file, 'w') as f:
            for country in processed_countries:
                f.write(f"{country}\n")

    def _get_pattern_directories(self):
        """Get list of directories containing pattern data
        
        Returns:
            list: List of directories containing pattern Parquet files
        """
        pattern_dirs = glob.glob(os.path.join(self.pattern_maker_output, "*"))
        return [d for d in pattern_dirs if os.path.isdir(d)]

    def _load_country_mappings(self):
        """Load country ID to name mappings from CSV file
        
        Returns:
            dict: Dictionary mapping country IDs to their names
        """
        self.logger.info("Loading country mappings...")
        countries_df = pd.read_csv(self.countries_csv)
        countries_df = countries_df.dropna(subset=['id', 'name_1'])
        countries_df = countries_df[countries_df['id'].apply(lambda x: isinstance(x, (int, float)))]
        countries_df['id'] = countries_df['id'].astype(int)
        country_id_to_name = dict(zip(countries_df['id'], countries_df['name_1']))
        self.logger.info(f"Loaded {len(country_id_to_name)} country mappings")
        return country_id_to_name

    def _process_country_directory(self, country_dir, country_id_to_name):
        """Process a single country directory to create symmetric patterns
        
        Args:
            country_dir (str): Path to the directory containing pattern data for a country
            country_id_to_name (dict): Dictionary mapping country IDs to names
        """
        country_name = os.path.basename(country_dir)
        self.logger.info(f"Processing country directory: {country_name}")
        
        # Read existing patterns for the country
        existing_patterns = self._read_country_patterns(country_dir)
        
        if existing_patterns.empty:
            self.logger.warning(f"No patterns found for country: {country_name}. Skipping...")
            return
        
        # Convert key columns to integers
        existing_patterns = self._convert_key_columns_to_int(existing_patterns)
        
        # Create symmetric patterns
        symmetric_patterns = self.swap_key_element_column_names(existing_patterns)
        
        # Track patterns that will be merged
        merged_pattern_keys = set()
        
        # Distribute symmetric patterns to their respective country directories
        merged_pattern_keys = self._distribute_symmetric_patterns(symmetric_patterns, country_id_to_name, merged_pattern_keys)
        
        # Remove merged patterns from original directory
        self._update_original_country_patterns(country_dir, existing_patterns, merged_pattern_keys)

    def _read_country_patterns(self, country_dir):
        """Read pattern data from Parquet files in a country directory
        
        Args:
            country_dir (str): Path to the directory containing pattern Parquet files
            
        Returns:
            pd.DataFrame: Combined DataFrame of all pattern data in the directory
        """
        parquet_files = glob.glob(os.path.join(country_dir, "*.parquet"))
        if not parquet_files:
            return pd.DataFrame()
        
        df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
        return self._convert_key_columns_to_int(df)

    def _convert_key_columns_to_int(self, df):
        """Convert key columns to integer data types
        
        Args:
            df (pd.DataFrame): DataFrame to convert
            
        Returns:
            pd.DataFrame: DataFrame with key columns as integers
        """
        if df.empty:
            return df
        
        key_columns = ['country_a_id', 'country_b_id', 'length', 'start_year_a', 'start_year_b']
        for col in key_columns:
            if col in df.columns:
                df[col] = df[col].astype(int)
        
        return df

    def swap_key_element_column_names(self, existing_patterns):
        """Create symmetric patterns by swapping country and year columns
        
        Args:
            existing_patterns (pd.DataFrame): Existing pattern data
            
        Returns:
            pd.DataFrame: DataFrame containing the symmetric patterns
        """
        self.logger.info("Creating symmetric patterns by swapping columns...")
        # Create a copy to avoid modifying the original DataFrame
        symmetric_patterns = existing_patterns.copy()
        
        # Swap country and year columns to create symmetric patterns
        symmetric_patterns.rename(columns={
            'country_a_id': 'country_b_id',
            'country_b_id': 'country_a_id',
            'start_year_a': 'start_year_b',
            'start_year_b': 'start_year_a'
        }, inplace=True)
        
        return self._convert_key_columns_to_int(symmetric_patterns)

    def _distribute_symmetric_patterns(self, symmetric_patterns, country_id_to_name, merged_pattern_keys):
        """Distribute symmetric patterns to their respective country directories
        
        Args:
            symmetric_patterns (pd.DataFrame): DataFrame containing symmetric patterns
            country_id_to_name (dict): Dictionary mapping country IDs to names
            merged_pattern_keys (set): Set to track keys of patterns that were merged
            
        Returns:
            set: Updated set of merged pattern keys
        """
        self.logger.info("Distributing symmetric patterns to country directories...")
        
        # Group symmetric patterns by new country_a_id (original country_b_id)
        grouped = symmetric_patterns.groupby('country_a_id')
        
        for new_country_id, group in grouped:
            new_country_name = country_id_to_name.get(new_country_id)
            if not new_country_name:
                self.logger.warning(f"Country ID {new_country_id} not found in country mapping. Skipping...")
                continue
            
            # Track pattern keys before merging
            pattern_keys = set(group.apply(
                lambda row: f"{row['country_a_id']}{row['country_b_id']}{row['length']}{row['start_year_a']}{row['start_year_b']}", 
                axis=1
            ))
            
            merged_pattern_keys.update(pattern_keys)
            
            self._update_country_patterns(new_country_name, group)
        
        return merged_pattern_keys

    def _update_original_country_patterns(self, country_dir, existing_patterns, merged_pattern_keys):
        """Update original country patterns by removing merged patterns
        
        Args:
            country_dir (str): Directory path for the country
            existing_patterns (pd.DataFrame): Original patterns for the country
            merged_pattern_keys (set): Set of keys for patterns that were merged
        """
        self.logger.info("Updating original country patterns by removing merged patterns...")
        
        if not merged_pattern_keys:
            self.logger.info("No patterns were merged. Skipping update...")
            return
        
        # Create pattern keys for existing patterns
        existing_patterns['pattern_key'] = existing_patterns.apply(
            lambda row: f"{row['country_a_id']}{row['country_b_id']}{row['length']}{row['start_year_a']}{row['start_year_b']}", 
            axis=1
        )
        
        # Filter out merged patterns
        updated_patterns = existing_patterns[~existing_patterns['pattern_key'].isin(merged_pattern_keys)].drop(columns=['pattern_key'])
        
        # Save the updated patterns
        self._save_updated_original_patterns(updated_patterns, country_dir)

    def _save_updated_original_patterns(self, updated_patterns, country_dir):
        """Save updated original patterns to Parquet files
        
        Args:
            updated_patterns (pd.DataFrame): DataFrame containing updated patterns
            country_dir (str): Directory path for the country
        """
        self.logger.info(f"Saving updated original patterns for country directory: {country_dir}")
        country_name = os.path.basename(country_dir)
        output_path = os.path.join(country_dir, f"patterns_{country_name}.parquet")
        
        # Remove existing pattern files to avoid duplication
        for file in glob.glob(os.path.join(country_dir, "*.parquet")):
            os.remove(file)
        
        if not updated_patterns.empty:
            updated_patterns.to_parquet(output_path, compression='brotli', index=False)
            self.logger.info(f"Saved updated original patterns to {output_path}")
        else:
            self.logger.info(f"No patterns remaining for {country_dir}. Directory will be empty.")

    def _update_country_patterns(self, new_country_name, new_patterns):
        """Update existing patterns for a country with new symmetric patterns
        
        Args:
            new_country_name (str): Name of the country to update
            new_patterns (pd.DataFrame): DataFrame containing new symmetric patterns
        """
        new_country_dir = os.path.join(self.pattern_maker_output, new_country_name)
        os.makedirs(new_country_dir, exist_ok=True)
        
        # Read existing patterns for the new country
        existing_files = glob.glob(os.path.join(new_country_dir, "*.parquet"))
        existing_patterns = pd.concat([pd.read_parquet(f) for f in existing_files], ignore_index=True) if existing_files else pd.DataFrame()
        
        # Convert key columns to integers
        existing_patterns = self._convert_key_columns_to_int(existing_patterns)
        new_patterns = self._convert_key_columns_to_int(new_patterns)
        
        # Combine existing and new patterns
        combined_patterns = self._properly_combine_patterns(existing_patterns, new_patterns)
        
        # Save the updated patterns
        self._save_combined_patterns(combined_patterns, new_country_dir, new_country_name)

    def _properly_combine_patterns(self, existing_patterns, new_patterns):
        """Combine existing patterns with new symmetric patterns, merging where appropriate
        
        Args:
            existing_patterns (pd.DataFrame): Existing pattern data
            new_patterns (pd.DataFrame): New symmetric patterns to add
            
        Returns:
            pd.DataFrame: Combined DataFrame of existing and new patterns
        """
        self.logger.info("Combining existing and new patterns with proper merging...")
        
        if existing_patterns.empty and new_patterns.empty:
            return pd.DataFrame()
        
        if existing_patterns.empty:
            return new_patterns
        
        if new_patterns.empty:
            return existing_patterns
        
        # Ensure key columns are integers
        existing_patterns = self._convert_key_columns_to_int(existing_patterns)
        new_patterns = self._convert_key_columns_to_int(new_patterns)
        
        # Create a temporary merge key
        existing_patterns['merge_key'] = existing_patterns.apply(
            lambda row: f"{row['country_a_id']}{row['country_b_id']}{row['length']}{row['start_year_a']}{row['start_year_b']}", 
            axis=1
        )
        new_patterns['merge_key'] = new_patterns.apply(
            lambda row: f"{row['country_a_id']}{row['country_b_id']}{row['length']}{row['start_year_a']}{row['start_year_b']}", 
            axis=1
        )
        
        # Find matching patterns
        merged = pd.merge(existing_patterns, new_patterns, on='merge_key', suffixes=('_existing', '_new'))
        
        if not merged.empty:
            # Process matched patterns
            matched_existing = merged[['country_a_id_existing', 'country_b_id_existing', 'length_existing', 
                                    'start_year_a_existing', 'start_year_b_existing', 'average_correlation_existing', 
                                    'scdi_count_existing']]
            matched_existing.columns = ['country_a_id', 'country_b_id', 'length', 'start_year_a', 'start_year_b', 
                                    'average_correlation', 'scdi_count']
            
            # Calculate new average and sum using .loc for explicit assignment
            matched_existing.loc[:, 'average_correlation'] = (matched_existing['average_correlation'] + 
                                                        merged['average_correlation_new']) / 2
            matched_existing.loc[:, 'scdi_count'] = matched_existing['scdi_count'] + merged['scdi_count_new']
            
            # Drop merge key from existing patterns
            existing_patterns = existing_patterns[~existing_patterns['merge_key'].isin(merged['merge_key'])].drop(columns=['merge_key'])
            
            # Combine results
            combined = pd.concat([existing_patterns, matched_existing, new_patterns[~new_patterns['merge_key'].isin(merged['merge_key'])].drop(columns=['merge_key'])])
        else:
            # No matches found, just concatenate
            combined = pd.concat([existing_patterns.drop(columns=['merge_key']), new_patterns.drop(columns=['merge_key'])])
        
        return combined

    def _save_combined_patterns(self, combined_patterns, country_dir, country_name):
        """Save combined patterns to Parquet files
        
        Args:
            combined_patterns (pd.DataFrame): DataFrame containing combined patterns
            country_dir (str): Directory path for the country
            country_name (str): Name of the country
        """
        self.logger.info(f"Saving updated patterns for country: {country_name}")
        output_path = os.path.join(country_dir, f"patterns_{country_name}.parquet")
        
        # Remove existing pattern files to avoid duplication
        for file in glob.glob(os.path.join(country_dir, "*.parquet")):
            os.remove(file)
        
        combined_patterns.to_parquet(output_path, compression='brotli', index=False)
        self.logger.info(f"Saved updated patterns to {output_path}")

# pattern_maker_output = r"/workspace/qgi\qgi-4.0\data\pattern_maker_output"
# countries_csv        = r"/workspace/qgi\qgi-4.0\data\countries.csv"
# generator = SymmetricPatternGenerator(pattern_maker_output, countries_csv)
# generator.create_symmetric_patterns()


class PatternMaker:
    """Class for creating and processing patterns from SCDI data"""

    def __init__(self, scdi_finder_output: str, pattern_maker_output: str, countries_csv: str):
        """
        Initialize the PatternMaker with the required paths.

        Args:
            scdi_finder_output: Path to the directory containing SCDI Parquet files.
            pattern_maker_output: Path to the directory where patterns will be saved.
            countries_csv: Path to the CSV file containing country information.
        """
        self.scdi_finder_output = scdi_finder_output
        self.pattern_maker_output = pattern_maker_output
        self.countries_csv = countries_csv

    def run(self):
        """Run the entire pattern creation and processing pipeline"""
        try:
            # Step 1: Create patterns by country
            logger.info("Starting pattern creation by country...")
            self.create_patterns_by_country()
            logger.info("Pattern creation by country completed successfully")

            # Step 2: Process all patterns (filter scdi_count == 1 and add PPS)
            logger.info("Starting processing of all patterns...")
            self._process_all_patterns()
            logger.info("Pattern processing completed successfully")

            # Step 3: Create symmetric patterns
            logger.info("Starting creation of symmetric patterns...")
            self._create_symmetric_patterns()
            logger.info("Symmetric pattern creation completed successfully")

            # Step 4: Create 'top_pps.parquet' with top patterns
            logger.info("Starting creation of 'top_pps.parquet'...")
            self.create_top_pps()
            logger.info("'top_pps.parquet' creation completed successfully")

            logger.info("Entire pipeline executed successfully")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise

    def _find_scdi_parquet_files_by_country(self, country_name):
        """
        Find all Parquet files for a specific country in the directory structure.

        Args:
            country_name: Name of the country to search for.

        Returns:
            List of Parquet file paths for the specified country.
        """
        parquet_files = []
        logger.info(f"Searching for Parquet files for country: {country_name}...")
        for root, dirs, files in os.walk(self.scdi_finder_output):
            # Skip directories ending with "_shrinked"
            if root.endswith("_shrinked"):
                continue

            for file in files:
                if file.endswith('.parquet') and str(file).startswith(str(country_name)):
                    full_path = os.path.join(root, file)
                    parquet_files.append(full_path)
                    logger.debug(f"Found Parquet file: {full_path}")

        if not parquet_files:
            logger.warning(f"No Parquet files found for country: {country_name}")

        logger.info(f"Found {len(parquet_files)} Parquet files for country: {country_name}")
        return parquet_files

    def _get_country_names(self):
        """Get the list of country names from the countries CSV file."""
        logger.info("Reading country names from CSV file...")
        countries_df = pd.read_csv(self.countries_csv)
        # Filter out non-string or missing values in 'name_1' column
        countries_df = countries_df.dropna(subset=['name_1'])
        countries_df = countries_df[countries_df['name_1'].apply(lambda x: isinstance(x, str))]
        country_names = countries_df['name_1'].tolist()
        logger.info(f"Found {len(country_names)} country names")
        return country_names

    def create_patterns_by_country(self):
        """Create patterns by grouping SCDIs for each country individually."""
        logger.info("Starting pattern creation by country...")

        # Get the list of country names
        country_names = self._get_country_names()

        for country_name in country_names:
            # Step 1: Find all Parquet files for the current country
            parquet_files = self._find_scdi_parquet_files_by_country(country_name)
            if not parquet_files:
                continue

            # Step 2: Create a Dask DataFrame from the Parquet files
            logger.info(f"Creating Dask DataFrame for country: {country_name}...")
            ddf = dd.read_parquet(parquet_files)

            # Step 3: Group by the specified columns and aggregate
            logger.info(f"Grouping and aggregating data for country: {country_name}...")
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

            # Step 4: Save the patterns to a Parquet file
            country_dir = os.path.join(self.pattern_maker_output, country_name)
            os.makedirs(country_dir, exist_ok=True)

            logger.info(f"Saving patterns for country {country_name}...")
            patterns_ddf.to_parquet(
                country_dir,
                compression="brotli",
                write_index=False
            )

            logger.info(f"Saved patterns for country {country_name}")

        logger.info("Pattern creation by country completed successfully")

    def _process_all_patterns(self):
        """Process all patterns by filtering scdi_count == 1 and adding PPS"""
        logger.info("Processing all patterns...")
        pattern_dirs = glob.glob(os.path.join(self.pattern_maker_output, "*"))
        pattern_dirs = [d for d in pattern_dirs if os.path.isdir(d)]

        for country_dir in pattern_dirs:
            parquet_files = glob.glob(os.path.join(country_dir, "*.parquet"))
            if not parquet_files:
                logger.warning(f"No Parquet files found in directory: {country_dir}. Skipping...")
                continue

            for file in parquet_files:
                logger.info(f"Processing file: {file}")
                df = pd.read_parquet(file)

                # Filter out rows where scdi_count == 1
                df = df[df['scdi_count'] != 1]

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
                logger.info(f"Saved processed Parquet file: {file}")

        logger.info("Pattern processing completed")

    def create_top_pps(self):
        """Create a Parquet file with top N patterns based on PPS"""
        logger.info(f"Creating 'top_pps.parquet' with top {TOP_PPS_PATTERNS_COUNT} patterns based on PPS...")
        pattern_dirs = glob.glob(os.path.join(self.pattern_maker_output, "*"))
        pattern_dirs = [d for d in pattern_dirs if os.path.isdir(d)]

        # Collect all Parquet files
        parquet_files = []
        for country_dir in pattern_dirs:
            files = glob.glob(os.path.join(country_dir, "*.parquet"))
            parquet_files.extend(files)

        if not parquet_files:
            logger.warning("No pattern files found. Skipping creation of 'top_pps.parquet'...")
            return

        # Read all Parquet files into a Dask DataFrame
        logger.info("Reading all Parquet files into a Dask DataFrame...")
        ddf = dd.read_parquet(parquet_files)

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

    def _create_symmetric_patterns(self):
        """Create symmetric patterns using the SymmetricPatternGenerator"""

        symmetric_generator = SymmetricPatternGenerator(
            pattern_maker_output=self.pattern_maker_output,
            countries_csv=self.countries_csv
        )
        symmetric_generator.create_symmetric_patterns()


creator = PatternMaker(
    scdi_finder_output=r"/workspace/qgi\qgi-4.0\data\scdi_finder_output",
    pattern_maker_output=r"/workspace/qgi\qgi-4.0\data\pattern_maker_output",
    countries_csv=r"/workspace/qgi\qgi-4.0\data\countries.csv"
)
creator.run()
