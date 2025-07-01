"""
SDCI Finder Pipeline

This script discovers pre-processed indicator data, performs a correlation
analysis to find all Single Dyadic Correlating Indexes (SDCIs), and saves
the results. It includes checkpointing to resume an interrupted run.
"""

import os
import glob
import logging
from dataclasses import dataclass
from typing import List
import numpy as np
import pandas as pd
from scipy.stats import pearsonr
import yaml


@dataclass(frozen=True)
class ScdiFinderConfig:
    """A validated configuration object for the pipeline."""
    input_path: str
    output_path: str
    countries_csv_path: str
    min_length: int
    correlation_threshold: float

    def __post_init__(self):
        """Validate paths after initialization."""
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input data path not found: {self.input_path}")
        if not os.path.exists(self.countries_csv_path):
            raise FileNotFoundError(f"Countries CSV not found: {self.countries_csv_path}")


@dataclass
class SDCI:
    """Represents a Single Dyadic Correlating Index."""
    country_a_id: int = 0
    country_b_id: int = 0
    length: int = 0
    start_year_a: int = 0
    start_year_b: int = 0
    correlation: float = 0.0


class ScdiFinder:
    """Finds SDCIs from a directory of indicator parquet files."""

    def __init__(self, config: ScdiFinderConfig):
        """Initializes the finder with a validated configuration."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        countries_df = pd.read_csv(self.config.countries_csv_path)
        self.country_name_to_id = {
            row["name_1"]: row["id"] for _, row in countries_df.iterrows()
        }

    def run(self):
        """Main method to run the entire SDCI finding pipeline."""
        self.logger.info("Starting SDCI Finder pipeline...")
        
        discovered_indicators = self._discover_indicators()
        total_indicators = len(discovered_indicators)
        self.logger.info(f"Discovered {total_indicators} indicators to process.")

        for index, (indicator_id, file_path) in enumerate(discovered_indicators, start=1):
            self.logger.info("-" * 40)
            # This log already includes the indicator ID, so it will look good.
            self.logger.info(
                "Processing indicator %d of %d (%.1f%%): %s",
                index, total_indicators, (index / total_indicators) * 100, indicator_id
            )
            self._process_one_indicator(indicator_id, file_path)
        
        self.logger.info("SDCI Finder pipeline completed successfully.")

    def _discover_indicators(self) -> List[tuple[str, str]]:
        """Finds all indicator parquet files in the input directory."""
        search_path = os.path.join(self.config.input_path, "**", "*.parquet")
        all_files = glob.glob(search_path, recursive=True)

        # Extract (indicator_id, path) tuples
        indicators = []
        for file_path in all_files:
            # Assumes indicator ID is the file name without extension
            indicator_id = os.path.splitext(os.path.basename(file_path))[0]
            indicators.append((indicator_id, file_path))

        return indicators

    def _process_one_indicator(self, indicator_id: str, file_path: str):
        """Loads data and finds SDCIs for a single indicator."""
        try:
            df = pd.read_parquet(file_path)
            countries = df["country"].unique()
            preprocessed_data = self._preprocess_data(df, countries)
            
            # MODIFIED: Added indicator_id for context
            self.logger.info(f"[{indicator_id}] Finding SDCIs for {len(preprocessed_data)} countries...")
            sdcis = self._find_sdcis_for_indicator(indicator_id, preprocessed_data)
            self.logger.info(f"[{indicator_id}] Found {len(sdcis):,} total SDCIs")

        except Exception as e:
            self.logger.error(f"[{indicator_id}] Processing failed: {e}", exc_info=True)

    def _preprocess_data(self, df: pd.DataFrame, countries: list) -> list:
        """Prepares data for each country, handling NaNs."""

        preprocessed_data = []
        for country in countries:
            country_data = df[df["country"] == country].sort_values("year")
            values = country_data["value"].values.copy()

            nan_indices = pd.isna(values)
            if np.sum(nan_indices) > 1:
                continue
            if np.sum(nan_indices) == 1:
                nan_pos = np.where(nan_indices)[0][0]
                if nan_pos == 0:
                    values[nan_pos] = values[nan_pos + 1]
                elif nan_pos == len(values) - 1:
                    values[nan_pos] = values[nan_pos - 1]
                else:
                    values[nan_pos] = (values[nan_pos - 1] + values[nan_pos + 1]) / 2

            preprocessed_data.append({
                "country": country,
                "data": country_data,
                "values": values,
            })
        return preprocessed_data

    def _find_sdcis_for_indicator(self, indicator_id: str, preprocessed_data: list) -> list:
        """Finds all SDCIs for a given indicator, with checkpointing."""
        all_sdcis = []
        indicator_output_dir = os.path.join(self.config.output_path, indicator_id)
        os.makedirs(indicator_output_dir, exist_ok=True)

        for i, country_a_data in enumerate(preprocessed_data):
            country_a_name = country_a_data["country"]

            country_output_path = os.path.join(indicator_output_dir, f"{country_a_name}.parquet")
            if os.path.exists(country_output_path):
                # MODIFIED: Added indicator_id for context
                self.logger.info(f"[{indicator_id}] Skipping '{country_a_name}': Output already exists.")
                continue

            # MODIFIED: Added indicator_id for context
            self.logger.info(f"[{indicator_id}] Processing country: {country_a_name} ({i+1}/{len(preprocessed_data)})")

            country_a_sdcis = []
            values_a = country_a_data["values"]

            for segment_len in range(self.config.min_length, len(values_a) + 1):
                for start_a in range(len(values_a) - segment_len + 1):
                    segment_a = values_a[start_a : start_a + segment_len]

                    for country_b_data in preprocessed_data[i + 1:]:
                        values_b = country_b_data["values"]
                        if len(values_b) < segment_len:
                            continue

                        for start_b in range(len(values_b) - segment_len + 1):
                            segment_b = values_b[start_b : start_b + segment_len]
                            if np.std(segment_a) == 0 or np.std(segment_b) == 0:
                                continue
                            correlation, _ = pearsonr(segment_a, segment_b)

                            if correlation >= self.config.correlation_threshold:
                                sdc = SDCI(
                                    country_a_id=self.country_name_to_id.get(country_a_name, 0),
                                    country_b_id=self.country_name_to_id.get(country_b_data["country"], 0),
                                    length=segment_len,
                                    correlation=correlation,
                                    start_year_a=country_a_data["data"].iloc[start_a]["year"],
                                    start_year_b=country_b_data["data"].iloc[start_b]["year"],
                                )
                                country_a_sdcis.append(sdc)

            if country_a_sdcis:
                # MODIFIED: Added indicator_id for context
                self.logger.info(f"[{indicator_id}] Saving {len(country_a_sdcis)} SDCIs for country '{country_a_name}'")
                sdcis_df = self._convert_sdcis_to_df(country_a_sdcis)
                sdcis_df.to_parquet(country_output_path, index=False, compression="brotli")
                all_sdcis.extend(country_a_sdcis)

        return all_sdcis

    def _convert_sdcis_to_df(self, sdcis: List[SDCI]) -> pd.DataFrame:
        """Converts a list of SDCI objects to a memory-optimized DataFrame."""
        if not sdcis:
            return pd.DataFrame()
        
        sdcis_df = pd.DataFrame([sdc.__dict__ for sdc in sdcis])

        # Convert to optimal data types
        return sdcis_df.astype({
            "country_a_id": "uint16",
            "country_b_id": "uint16",
            "length": "uint16",
            "start_year_a": "uint16",
            "start_year_b": "uint16",
            "correlation": "float32",
        })


def load_config(config_path="config.yaml") -> ScdiFinderConfig:
    """Loads YAML config file into a dataclass object."""
    # Path is relative to the script's location
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_full_path = os.path.join(current_dir, config_path)

    with open(config_full_path, "r") as f:
        config_dict = yaml.safe_load(f)

    return ScdiFinderConfig(**config_dict)


def main():
    """Main function to initialize and run the SDCI Finder pipeline."""
    # MODIFIED: Simplified the log format to just the message
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    try:
        config = load_config()
        pipeline = ScdiFinder(config)
        pipeline.run()
    except Exception as e:
        logging.getLogger(__name__).critical("Pipeline failed with an unhandled exception.", exc_info=True)
        raise


if __name__ == "__main__":
    main()
