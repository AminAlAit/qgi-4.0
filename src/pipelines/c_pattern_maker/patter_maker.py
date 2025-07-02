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
        self.logger.info("Step 2: Generating final top patterns report...")
        self._create_top_patterns_report()
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

    def _create_top_patterns_report(self):
        """
        Loads all base patterns, creates symmetric counterparts in memory,
        merges them using a canonical key, calculates significance,
        and saves the final top N report.
        """
        self.logger.info("Aggregating all patterns to find top results...")
        
        base_pattern_files = glob.glob(os.path.join(self.config.output_patterns_path, "*", "patterns.parquet"))
        
        if not base_pattern_files:
            self.logger.warning("No base pattern files found to generate report.")
            return

        ddf = dd.read_parquet(base_pattern_files)

        # Create canonical keys for country pairs and years to ensure proper merging
        ddf['ck_country_1'] = ddf[['country_a_id', 'country_b_id']].min(axis=1)
        ddf['ck_country_2'] = ddf[['country_a_id', 'country_b_id']].max(axis=1)
        ddf['ck_year_1'] = ddf[['start_year_a', 'start_year_b']].min(axis=1)
        ddf['ck_year_2'] = ddf[['start_year_a', 'start_year_b']].max(axis=1)
        
        grouping_keys = ['ck_country_1', 'ck_country_2', 'length', 'ck_year_1', 'ck_year_2']

        # Group by the canonical keys and aggregate
        merged_ddf = ddf.groupby(grouping_keys).agg(
            average_correlation=('average_correlation', 'mean'),
            scdi_count=('scdi_count', 'sum')
        ).reset_index()

        df = merged_ddf.compute()
        
        df.rename(columns={
            'ck_country_1': 'country_a_id', 'ck_country_2': 'country_b_id',
            'ck_year_1': 'start_year_a', 'ck_year_2': 'start_year_b'
        }, inplace=True)

        df['significance'] = (
            0.4 * df['average_correlation'] +
            0.4 * df['scdi_count'] +
            0.2 * df['length']
        )
        
        top_patterns = df.nlargest(self.config.top_n_patterns, 'significance')
        self.logger.info(f"Saving top {len(top_patterns)} patterns to {self.config.top_patterns_report_path}")
        top_patterns.to_parquet(self.config.top_patterns_report_path, index=False, compression='brotli')


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
