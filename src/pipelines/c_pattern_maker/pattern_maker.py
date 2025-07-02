"""
Pattern Maker Pipeline

This pipeline performs three main functions:
1.  Aggregates Single Dyadic Correlating Indexes (SDCIs) into "patterns".
2.  Calculates symmetric patterns to ensure relationships are bidirectional.
3.  Generates a final report of the top N most significant patterns.
"""

import os
import glob
import logging
from dataclasses import dataclass
import pandas as pd
import dask.dataframe as dd
import pyarrow.parquet as pq
from pyarrow import compute as pc
import yaml


@dataclass(frozen=True)
class PatternMakerConfig:
    """Validated configuration for the PatternMaker pipeline."""
    input_scdi_path: str
    output_patterns_path: str
    top_patterns_report_path: str
    countries_csv_path: str
    top_n_patterns: int


class PatternMaker:
    """Orchestrates the creation and processing of patterns from SCDI data."""

    def __init__(self, config: PatternMakerConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.country_id_to_name = self._load_country_mappings()

    def run(self):
        """Executes the full pattern generation pipeline."""
        self.logger.info("Starting Pattern Maker pipeline...")
        self.logger.info("Step 1: Creating/Verifying base patterns from SCDI files...")
        self._create_patterns_from_scdis()
        self.logger.info("Pattern Maker pipeline completed successfully.")

    def _load_country_mappings(self) -> dict:
        """Loads country ID to name mappings from the CSV file."""
        df = pd.read_csv(self.config.countries_csv_path)
        return dict(zip(df['id'], df['name_1']))

    def _create_patterns_from_scdis(self):
        """Groups all available SCDIs by country and aggregates them into patterns."""
        country_dirs = glob.glob(os.path.join(self.config.input_scdi_path, "*", "*"))
        country_names = {os.path.basename(path).replace('.parquet', '') for path in country_dirs}
        total_countries = len(country_names)

        for i, country_name in enumerate(country_names):
            output_dir = os.path.join(self.config.output_patterns_path, country_name)
            output_file = os.path.join(output_dir, "patterns.parquet")

            # MODIFIED: Added percentage calculation to the log message
            progress_percent = ((i + 1) / total_countries) * 100
            self.logger.info(f"({i+1}/{total_countries} | {progress_percent:.1f}%) Aggregating patterns for '{country_name}'")
            scdi_files = glob.glob(os.path.join(self.config.input_scdi_path, "*", f"{country_name}.parquet"))

            if not scdi_files:
                continue

            table = pq.read_table(scdi_files)
            if table.num_rows == 0:
                continue

            # Aggregate all SCDIs into patterns
            agg_table = table.group_by(['country_a_id', 'country_b_id', 'length', 'start_year_a', 'start_year_b']) \
                             .aggregate([('correlation', 'mean'), ('correlation', 'count')]) \
                             .rename_columns(['country_a_id', 'country_b_id', 'length', 'start_year_a', 'start_year_b', 'average_correlation', 'scdi_count'])

            if 'scdi_count' in agg_table.column_names:
                agg_table = agg_table.filter(pc.greater(agg_table['scdi_count'], 1))  # pylint: disable=no-member

            if agg_table.num_rows > 0:
                os.makedirs(output_dir, exist_ok=True)
                pq.write_table(agg_table, output_file, compression='brotli')
            else:
                self.logger.info(f"No patterns with scdi_count > 1 found for '{country_name}'. No file written.")


def load_config(config_path="config.yaml") -> PatternMakerConfig:
    """Loads YAML config file into a dataclass object."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_full_path = os.path.join(current_dir, config_path)
    with open(config_full_path, "r") as f:
        config_dict = yaml.safe_load(f)
    return PatternMakerConfig(**config_dict)

def main():
    """Main function to initialize and run the Pattern Maker pipeline."""
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    try:
        config = load_config()
        pipeline = PatternMaker(config)
        pipeline.run()
    except Exception:
        logging.getLogger(__name__).critical("Pipeline failed with an unhandled exception.", exc_info=True)
        raise

if __name__ == "__main__":
    main()
