"""Pattern Extraction Application"""

import logging
import os
import yaml
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import pyarrow.parquet as pq
from pattern import Pattern  # pylint: disable=import-error


logger = logging.getLogger(__name__)


class PatternExtractor:
    """TODO"""

    def __init__(self, data_path):
        self.data_path = data_path

    def load_indicator_data(self, indicator_id):
        """Load cleaned indicator data from Parquet file"""
        try:
            # Construct the correct path to the Parquet file
            indicator_dir = os.path.join(self.data_path, indicator_id)
            parquet_path = os.path.join(indicator_dir, f"{indicator_id}.parquet")

            table = pq.read_table(parquet_path)
            return table.to_pandas()
        except Exception as e:
            logger.error(f"Error loading data for {indicator_id}: {str(e)}")
            raise

    def find_patterns(self, indicator_id, min_length=3, max_length=5, correlation_threshold=0.9):
        """Find patterns in the indicator data"""
        df = self.load_indicator_data(indicator_id)
        patterns = []
        countries = df["country"].unique()

        # Calculate total number of comparisons for progress tracking
        total_comparisons = 0
        for country_a in countries:
            country_a_data = df[df["country"] == country_a].sort_values("year")
            values_a = country_a_data["value"].values

            # Handle NaN values
            nan_indices = np.isnan(values_a)
            if np.sum(nan_indices) > 1:
                continue

            # Fill single NaN if present
            if np.sum(nan_indices) == 1:
                nan_pos = np.where(nan_indices)[0][0]
                if nan_pos == 0:
                    values_a[nan_pos] = values_a[nan_pos + 1]
                elif nan_pos == len(values_a) - 1:
                    values_a[nan_pos] = values_a[nan_pos - 1]
                else:
                    values_a[nan_pos] = (values_a[nan_pos - 1] + values_a[nan_pos + 1]) / 2

            for segment_length in range(min_length, max_length + 1):
                if len(values_a) < segment_length:
                    continue

                for start_idx in range(len(values_a) - segment_length + 1):
                    for country_b in countries:
                        if country_a == country_b:
                            continue

                        country_b_data = df[df["country"] == country_b].sort_values("year")
                        values_b = country_b_data["value"].values

                        # Similar NaN handling for country B
                        nan_indices_b = np.isnan(values_b)
                        if np.sum(nan_indices_b) > 1:
                            continue

                        if np.sum(nan_indices_b) == 1:
                            nan_pos_b = np.where(nan_indices_b)[0][0]
                            if nan_pos_b == 0:
                                values_b[nan_pos_b] = values_b[nan_pos_b + 1]
                            elif nan_pos_b == len(values_b) - 1:
                                values_b[nan_pos_b] = values_b[nan_pos_b - 1]
                            else:
                                values_b[nan_pos_b] = (
                                    values_b[nan_pos_b - 1] + values_b[nan_pos_b + 1]
                                ) / 2

                        if len(values_b) < segment_length:
                            continue

                        total_comparisons += (len(values_b) - segment_length + 1)

        # Reset progress tracking
        progress = 0
        logger.info("Total comparisons for %s: %d", indicator_id, total_comparisons)

        for country_a in countries:
            country_a_data = df[df["country"] == country_a].sort_values("year")
            values_a = country_a_data["value"].values

            # Handle NaN values
            nan_indices = np.isnan(values_a)
            if np.sum(nan_indices) > 1:
                continue

            # Fill single NaN if present
            if np.sum(nan_indices) == 1:
                nan_pos = np.where(nan_indices)[0][0]
                if nan_pos == 0:
                    values_a[nan_pos] = values_a[nan_pos + 1]
                elif nan_pos == len(values_a) - 1:
                    values_a[nan_pos] = values_a[nan_pos - 1]
                else:
                    values_a[nan_pos] = (values_a[nan_pos - 1] + values_a[nan_pos + 1]) / 2

            for segment_length in range(min_length, max_length + 1):
                if len(values_a) < segment_length:
                    continue

                for start_idx in range(len(values_a) - segment_length + 1):
                    segment_a = values_a[start_idx:start_idx + segment_length]

                    for country_b in countries:
                        if country_a == country_b:
                            continue

                        country_b_data = df[df["country"] == country_b].sort_values("year")
                        values_b = country_b_data["value"].values

                        # Similar NaN handling for country B
                        nan_indices_b = np.isnan(values_b)
                        if np.sum(nan_indices_b) > 1:
                            continue

                        if np.sum(nan_indices_b) == 1:
                            nan_pos_b = np.where(nan_indices_b)[0][0]
                            if nan_pos_b == 0:
                                values_b[nan_pos_b] = values_b[nan_pos_b + 1]
                            elif nan_pos_b == len(values_b) - 1:
                                values_b[nan_pos_b] = values_b[nan_pos_b - 1]
                            else:
                                values_b[nan_pos_b] = (
                                    values_b[nan_pos_b - 1] + values_b[nan_pos_b + 1]
                                ) / 2

                        if len(values_b) < segment_length:
                            continue

                        for start_idx_b in range(len(values_b) - segment_length + 1):
                            segment_b = values_b[start_idx_b:start_idx_b + segment_length]

                            # Calculate correlation
                            correlation, _ = pearsonr(segment_a, segment_b)

                            if correlation >= correlation_threshold:
                                pattern = Pattern()
                                pattern.indicator_id = indicator_id
                                pattern.country_a = country_a
                                pattern.country_b = country_b
                                pattern.start_year_a = country_a_data.iloc[start_idx]['year']
                                pattern.start_year_b = country_b_data.iloc[start_idx_b]['year']
                                pattern.length = segment_length
                                pattern.correlation = correlation
                                pattern.data_segment_a = segment_a
                                pattern.data_segment_b = segment_b

                                patterns.append(pattern)

                            # Update and log progress
                            progress += 1
                            if progress % 1000 == 0 or progress == total_comparisons:
                                percent_complete = (progress / total_comparisons) * 100
                                logger.info("Progress: %.2f%% (%d of %d comparisons)",
                                            percent_complete, progress, total_comparisons)

        return patterns


def main():
    """Main function to run the pattern extraction pipeline"""
    try:
        # Load configuration
        config = load_config()

        # Initialize pattern extractor
        extractor = PatternExtractor(config["input_path"])

        # Process each indicator with progress tracking
        indicators = config["indicators"]
        total_indicators = len(indicators)

        for index, indicator_id in enumerate(indicators, start=1):
            # Calculate progress percentage
            progress_percent = (index / total_indicators) * 100

            # Print progress message
            logger.info(
                "Processing indicator %d of %d (%.1f%% complete): %s",
                index, total_indicators, progress_percent, indicator_id
            )

            # Find patterns for the indicator
            patterns = extractor.find_patterns(
                indicator_id=indicator_id,
                min_length=config["min_length"],
                max_length=config["max_length"],
                correlation_threshold=config["correlation_threshold"]
            )

            # Save patterns
            output_dir = os.path.join(config["output_path"], indicator_id)
            os.makedirs(output_dir, exist_ok=True)

            # Save patterns to Parquet
            pattern_df = pd.DataFrame([pattern.__dict__ for pattern in patterns])
            pattern_df.to_parquet(
                os.path.join(output_dir, f"{indicator_id}_patterns.parquet"),
                index=False,
                compression="snappy"
            )

            logger.info("Found %d patterns for indicator %s", len(patterns), indicator_id)

        logger.info("Pattern extraction pipeline completed successfully")

    except Exception as e:
        logger.error("Pattern extraction pipeline failed: %s", str(e))
        raise


def load_config(config_path="config.yaml"):
    """TODO: move to config.py"""
    # Get the directory of the current script (app.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to config.yaml
    config_full_path = os.path.join(current_dir, config_path)

    with open(config_full_path, "r") as f:
        config = yaml.safe_load(f)
    return config


if __name__ == "__main__":
    main()
