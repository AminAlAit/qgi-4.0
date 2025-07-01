"""
A self-contained ETL pipeline for fetching, transforming, and storing indicators
from the World Bank Open Data API.

This module orchestrates the entire ETL (Extract, Transform, Load) process to
build a clean, analysis-ready dataset of global indicators.

Functionality:
-------------
- **Extract**: Connects to the World Bank API (`wbgapi`) to download data and
  metadata for a predefined list of indicators.
- **Transform**: Performs essential data cleaning and reshaping operations:
    - Pivots data from a wide format (years as columns) to a long format.
    - Standardizes country names using a provided mapping file.
    - Removes unmappable regions and aggregates.
- **Load**: Saves the transformed data for each indicator into a separate,
  compressed Parquet file for efficient storage and access.

Structure:
----------
The pipeline is organized into distinct classes, each handling one stage of the ETL process:
- `Extractor`: Handles all API interactions and data fetching.
- `Transformer`: Encapsulates all data cleaning and transformation logic.
- `Loader`: Manages the saving of final data to the filesystem.
- `Pipeline`: The main orchestrator that initializes and executes the stages in sequence.
- `Indicator`: A data class representing a single indicator and its associated information.

Usage:
------
1.  Configure the `wb_config.yaml` file with the desired indicator IDs, output paths,
    and the location of the countries mapping CSV.
2.  Ensure all required dependencies are installed:
    `pip install pandas wbgapi pyarrow numpy PyYAML`
3.  Execute the script from your terminal:
    `python pipeline.py`

The pipeline will then run from start to finish, logging its progress to the console
and placing the final Parquet files in the specified output directory.
"""

import os
import logging
from dataclasses import dataclass
from pprint import pprint
from typing import List, Dict, Tuple
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import wbgapi as wb


@dataclass(frozen=True)
class PipelineConfig:
    """A structured and validated representation of the pipeline's configuration."""

    output_path: str
    countries_csv_path: str
    output_format: str
    indicators: List[str]
    file_size_limit: int = 100000000 # Example of a default value

    def __post_init__(self):
        """Perform validation after the object is created."""

        if not self.indicators:
            raise ValueError("Configuration error: 'indicators' list cannot be empty.")
        # Ensure the parent directory for the output path exists
        parent_dir = os.path.dirname(self.output_path)
        if parent_dir and not os.path.exists(parent_dir):
                 raise FileNotFoundError(f"Parent directory for output_path does not exist: {self.output_path}")
        if not os.path.exists(self.countries_csv_path):
            raise FileNotFoundError(f"Countries mapping CSV not found at: {self.countries_csv_path}")


@dataclass(frozen=True)
class ETLResult:
    """A summary of the completed ETL pipeline run."""

    processed_indicators: List[str]
    unmatched_economies: Dict[str, List[str]]
    output_location: str
    indicators_with_no_data: List[str]
    status: str = "Success"


class Indicator:
    """Data class to hold all information for a single indicator."""

    id: str = ""
    data: pd.DataFrame = pd.DataFrame()
    indicator_name: str = ""
    concept: str = ""
    license_type: str = ""
    license_url: str = ""
    long_definition: str = ""
    othernotes: str = ""
    periodicity: str = ""
    related_source_links: str = ""
    source: str = ""
    topic: str = ""


class Extractor:
    """Extracts data from the World Bank API for specified indicators."""

    def __init__(self):
        logging.info("Extractor initialized.")

    def execute(self, indicator_ids: List[str]) -> List[Indicator]:
        """Runs the extraction process for a list of indicator IDs."""

        logging.info("Starting data extraction.")
        try:
            extracted_indicators = [self._get_one_indicator(indicator_id) for indicator_id in indicator_ids]
            logging.info("Data extraction completed successfully.")
            return extracted_indicators
        except Exception as e:
            logging.error(f"Data extraction failed: {e}")
            raise

    def _get_one_indicator(self, indicator_id: str) -> Indicator:
        """Fetches data and metadata for a single indicator from the World Bank API."""

        logging.info(f"Fetching indicator: {indicator_id}")
        indicator = Indicator()
        indicator_metadata_obj = wb.series.metadata.get(indicator_id)
        metadata = indicator_metadata_obj.metadata
        indicator.id = indicator_id
        indicator.data = wb.data.DataFrame(indicator_id)
        get = lambda key: metadata.get(key, "")
        indicator.indicator_name = get("IndicatorName")
        indicator.concept = indicator_metadata_obj.concept
        indicator.license_type = get("License_Type")
        indicator.license_url = get("License_URL")
        indicator.long_definition = get("Longdefinition")
        indicator.othernotes = get("Othernotes")
        indicator.periodicity = get("Periodicity")
        indicator.related_source_links = get("Relatedsourcelinks")
        indicator.source = get("Source")
        indicator.topic = get("Topic")
        return indicator


class Transformer:
    """Transforms the extracted indicator data into a clean, long format."""

    def __init__(self, countries_csv_path: str):
        logging.info("Transformer initialized.")
        self._country_mapping = self._create_country_mapping(countries_csv_path)

    def _create_country_mapping(self, csv_path: str) -> Dict[str, str]:
        """Creates a comprehensive mapping from various country codes to a canonical name."""
        reference_df = pd.read_csv(csv_path)
        reference_df = reference_df.loc[:, ~reference_df.columns.str.contains("Unnamed")]
        if 'name_1' not in reference_df.columns:
            raise KeyError("The key 'name_1' is missing in the country reference file.")
        mapping = {}
        for col in reference_df.columns:
            if col != 'name_1':
                temp_map = reference_df.dropna(subset=[col]).set_index(col)['name_1'].to_dict()
                mapping.update(temp_map)
        return mapping

    def execute(self, indicators: List[Indicator]) -> Tuple[List[Indicator], Dict[str, List[str]]]:
        """
        Runs the transformation process on a list of indicators.

        Returns:
            A tuple containing:
            - A list of transformed Indicator objects.
            - A dictionary reporting unmatched economies for each indicator.
        """

        logging.info("Starting data transformation.")
        unmatched_report = {}
        try:
            transformed_indicators = []
            for ind in indicators:
                transformed_ind, unmatched_list = self._transform_one(ind)
                transformed_indicators.append(transformed_ind)
                if unmatched_list:
                    unmatched_report[ind.id] = unmatched_list
            
            logging.info("Data transformation completed successfully.")
            return transformed_indicators, unmatched_report
        except Exception as e:
            logging.error(f"Data transformation failed: {e}")
            raise

    # MODIFIED: This method now returns the list of unmatched values.
    def _transform_one(self, indicator: Indicator) -> Tuple[Indicator, List[str]]:
        """Applies all transformation steps to a single indicator's DataFrame."""
        df = indicator.data.copy()
        df = self._remove_all_nan_year_columns(df)
        df = self._pivot_dataframe(df)
        df = self._rename_columns(df)
        df, unmatched = self._clean_country_names(df)
        df = self._delete_rows_by_country(df, unmatched)
        indicator.data = df
        return indicator, unmatched

    def _remove_all_nan_year_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Removes year columns where all country values are NaN."""
        return df.dropna(axis='columns', how='all')

    def _pivot_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pivots the DataFrame from wide (years as columns) to long format."""
        df["economy"] = df.index
        df_long = pd.melt(df, id_vars=['economy'], var_name='year', value_name='value')
        df_long['year'] = df_long['year'].str.extract(r'(\d+)').astype(int)
        df_long = df_long.sort_values(by=['economy', 'year']).reset_index(drop=True)
        return df_long

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renames 'economy' to 'country' for clarity."""
        return df.rename(columns={'economy': 'country'})

    def _clean_country_names(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Replaces country codes with full names using the pre-loaded mapping."""
        df['country'] = df['country'].map(self._country_mapping).fillna(df['country'])
        all_canonical_names = set(self._country_mapping.values())
        unmatched_values = list(set(value for value in df['country'] if value not in all_canonical_names))
        return df, unmatched_values

    def _delete_rows_by_country(self, df: pd.DataFrame, countries_to_delete: List[str]) -> pd.DataFrame:
        """Deletes rows for countries and aggregates that could not be mapped to a canonical name."""
        if countries_to_delete:
            logging.warning(f"Deleting rows for {len(countries_to_delete)} unmatched economies: {countries_to_delete}")
        return df[~df['country'].isin(countries_to_delete)]


class Loader:
    """Loads transformed data into storage, such as Parquet files."""

    def __init__(self):
        logging.info("Loader initialized.")

    def execute(self, indicators: List[Indicator], output_path: str, output_format: str) -> List[str]:
        """
        Saves each indicator's DataFrame to a Parquet file.

        Returns:
            A list of indicator IDs that had no data and were skipped.
        """

        if output_format.lower() != 'parquet':
            raise ValueError(f"Unsupported output format: {output_format}")

        logging.info(f"Starting data loading to {output_path}")
        skipped_indicators = []
        try:
            for indicator in indicators:
                output_dir = os.path.join(output_path, indicator.id)
                os.makedirs(output_dir, exist_ok=True)
                if indicator.data.empty:
                    logging.warning(f"Indicator {indicator.id} has no data to save. Skipping.")
                    skipped_indicators.append(indicator.id)
                    continue
                table = pa.Table.from_pandas(indicator.data)
                output_file = os.path.join(output_dir, f"{indicator.id}.parquet")
                pq.write_table(table, output_file, compression="snappy")
            logging.info("Data loading completed successfully.")
            return skipped_indicators
        except Exception as e:
            logging.error(f"Data loading failed: {e}")
            raise


class Pipeline:
    """Orchestrates the entire ETL (Extract, Transform, Load) process."""

    def __init__(self, config_path: str = "wb_config.yaml"):
        """Initializes the pipeline by loading config and setting up ETL stages."""

        self.config: PipelineConfig = self._load_config(config_path)
        self.extractor = Extractor()
        self.transformer = Transformer(countries_csv_path=self.config.countries_csv_path)
        self.loader = Loader()

    def _load_config(self, config_path: str) -> PipelineConfig:
        """Loads the YAML configuration file and returns a validated Config object."""

        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_full_path = os.path.join(script_dir, config_path)
        logging.info(f"Loading configuration from: {config_full_path}")
        with open(config_full_path, "r") as f:
            config_dict = yaml.safe_load(f)
        return PipelineConfig(**config_dict)

    def run(self) -> ETLResult:
        """Executes the ETL pipeline from start to finish."""

        logging.info("ETL pipeline starting.")
        try:
            # 1. Extract Stage
            indicator_ids = self.config.indicators
            extracted_indicators = self.extractor.execute(indicator_ids)

            # 2. Transform Stage
            transformed_indicators, unmatched_report = self.transformer.execute(
                extracted_indicators
            )

            # 3. Load Stage
            skipped_indicators = self.loader.execute(
                indicators=transformed_indicators,
                output_path=self.config.output_path,
                output_format=self.config.output_format
            )

            logging.info("ETL pipeline completed successfully.")

            # 4. Create and return the result report
            return ETLResult(
                processed_indicators=[ind.id for ind in transformed_indicators],
                unmatched_economies=unmatched_report,
                output_location=self.config.output_path,
                indicators_with_no_data=skipped_indicators,
                status="Success"
            )

        except Exception as e:
            logging.error(f"ETL pipeline failed: {str(e)}")
            raise


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
pipeline = Pipeline(config_path="wb_config.yaml")
result = pipeline.run()

print("\n----- ETL Pipeline Result -----")
pprint(result, compact=True)
print("-----------------------------\n")
