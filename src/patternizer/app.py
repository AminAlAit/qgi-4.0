"""SDCI Extraction Application"""

import logging
import os
import yaml
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")


class SDCI:
    """Class representing a Single Dyadic Correlating Index"""

    def __init__(self):
        self.country_a = ""
        self.country_b = ""
        self.length = 0
        self.start_year_a = 0
        self.start_year_b = 0
        self.correlation = 0.0
        self.data_segment_a = None
        self.data_segment_b = None


class SDCIExtractor:
    """Class for extracting SDCIs from indicator data"""

    def __init__(self, data_path, output_path):
        self.data_path = data_path
        self.output_path = output_path

    def load_indicator_data(self, indicator_id):
        """Load cleaned indicator data from Parquet file"""
        try:
            indicator_dir = os.path.join(self.data_path, indicator_id)
            parquet_path = os.path.join(indicator_dir, f"{indicator_id}.parquet")
            table = pq.read_table(parquet_path)
            return table.to_pandas()
        except Exception as e:
            logger.error(f"Error loading data for {indicator_id}: {str(e)}")
            raise

    def find_sdcis(self, indicator_id, min_length=3, max_length=5, correlation_threshold=0.9):
        """Main method to find SDCIs in the indicator data"""
        df = self.load_indicator_data(indicator_id)
        countries = df["country"].unique()

        # Preprocess data for each country
        preprocessed_data = self._preprocess_data(df, countries)

        # Calculate total number of comparisons for progress tracking
        total_comparisons = self._calculate_total_comparisons(
            preprocessed_data, min_length, max_length
        )

        # Find SDCIs with progress tracking
        sdcis = self._find_sdcis(
            indicator_id,
            preprocessed_data,
            total_comparisons,
            min_length,
            max_length,
            correlation_threshold
        )

        return sdcis

    def _preprocess_data(self, df, countries):
        """Preprocess data for each country"""
        preprocessed_data = []

        for country in countries:
            country_data = df[df["country"] == country].sort_values("year")
            values = country_data["value"].values.copy()

            # Handle NaN values
            nan_indices = np.isnan(values)
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
                "values": values
            })

        return preprocessed_data

    def _calculate_total_comparisons(self, preprocessed_data, min_length, max_length):
        """Calculate total number of comparisons for progress tracking"""
        total_comparisons = 0

        for i, country_a in enumerate(preprocessed_data):
            values_a = country_a["values"]

            for segment_length in range(min_length, max_length + 1):
                if len(values_a) < segment_length:
                    continue

                num_segments_a = len(values_a) - segment_length + 1
                num_segments_b = sum(
                    len(country_b["values"]) - segment_length + 1
                    for country_b in preprocessed_data[i+1:]
                    if len(country_b["values"]) >= segment_length
                )
                total_comparisons += num_segments_a * num_segments_b

        return total_comparisons

    def _find_sdcis(
        self,
        indicator_id,
        preprocessed_data,
        total_comparisons,
        min_length,
        max_length,
        correlation_threshold
    ):
        """Find SDCIs with progress tracking and incremental saving"""
        sdcis = []
        progress = 0
        country_a_progress = {}
        logger.info("Total comparisons: %d", total_comparisons)

        # Create indicator output directory
        indicator_output_dir = os.path.join(self.output_path, indicator_id)
        os.makedirs(indicator_output_dir, exist_ok=True)

        # Check if there's an existing SDCI file
        sdcis_df = pd.DataFrame()
        existing_countries = set()
        sdcis_file = os.path.join(indicator_output_dir, f"{indicator_id}_sdcis.parquet")
        if os.path.exists(sdcis_file):
            sdcis_df = pd.read_parquet(sdcis_file)
            existing_countries = set(sdcis_df["country_a"].unique())
            logger.info("Resuming from existing SDCIs for %s", indicator_id)

        # Filter out countries already processed
        preprocessed_data = [
            country for country in preprocessed_data
            if country["country"] not in existing_countries
        ]

        for i, country_a in enumerate(preprocessed_data):
            values_a = country_a["values"]
            country_a_name = country_a["country"]
            country_a_sdcis = []

            # Calculate total comparisons for this country
            total_comparisons_country = 0
            for segment_length in range(min_length, max_length + 1):
                if len(values_a) < segment_length:
                    continue
                num_segments_a = len(values_a) - segment_length + 1
                for country_b in preprocessed_data[i+1:]:
                    if len(country_b["values"]) >= segment_length:
                        total_comparisons_country += (
                            len(values_a) - segment_length + 1
                        ) * (len(country_b["values"]) - segment_length + 1)

            country_a_progress[country_a_name] = {
                "total": total_comparisons_country,
                "completed": 0
            }

            for segment_length in range(min_length, max_length + 1):
                if len(values_a) < segment_length:
                    continue

                num_segments_a = len(values_a) - segment_length + 1

                for start_idx in range(num_segments_a):
                    segment_a = values_a[start_idx:start_idx + segment_length]

                    for country_b in preprocessed_data[i+1:]:
                        values_b = country_b["values"]
                        country_b_name = country_b["country"]

                        if len(values_b) < segment_length:
                            continue

                        num_segments_b = len(values_b) - segment_length + 1

                        for start_idx_b in range(num_segments_b):
                            segment_b = values_b[start_idx_b:start_idx_b + segment_length]

                            # Calculate correlation
                            correlation, _ = pearsonr(segment_a, segment_b)

                            if correlation >= correlation_threshold:
                                sdc = SDCI()
                                sdc.country_a = country_a_name
                                sdc.country_b = country_b_name
                                sdc.length = segment_length
                                sdc.correlation = correlation
                                sdc.start_year_a = country_a["data"].iloc[start_idx]['year']
                                sdc.start_year_b = country_b["data"].iloc[start_idx_b]['year']
                                sdc.data_segment_a = segment_a
                                sdc.data_segment_b = segment_b
                                country_a_sdcis.append(sdc)

                            # Update progress counters
                            progress += 1
                            country_a_progress[country_a_name]["completed"] += 1

                            # Calculate progress percentages
                            overall_percent = (progress / total_comparisons) * 100
                            country_a_percent = (
                                country_a_progress[country_a_name]["completed"] / country_a_progress[country_a_name]["total"]
                            ) * 100 if country_a_progress[country_a_name]["total"] > 0 else 0

                            # Log progress
                            if progress % 1000 == 0 or progress == total_comparisons:
                                logger.info(
                                    "%s | %s: %.2f%% | %d (%.2f%%)",
                                    indicator_id,
                                    country_a_name,
                                    country_a_percent,
                                    len(sdcis) + len(country_a_sdcis),
                                    overall_percent
                                )

            # Save SDCIs for this country after processing
            if country_a_sdcis:
                country_a_sdcis_df = pd.DataFrame([sdc.__dict__ for sdc in country_a_sdcis])
                combined_df = pd.concat([sdcis_df, country_a_sdcis_df], ignore_index=True)
                combined_df.to_parquet(
                    sdcis_file,
                    index=False,
                    compression="snappy"
                )
                sdcis_df = combined_df
                logger.info("Saved SDCIs for country %s", country_a_name)

        return sdcis_df.to_dict("records")

    def _create_sdci(
        self,
        country_a,
        country_b,
        length,
        correlation,
        start_year_a,
        start_year_b,
        segment_a,
        segment_b
    ):
        """Create a new SDCI object"""
        sdc = SDCI()
        sdc.country_a = country_a
        sdc.country_b = country_b
        sdc.length = length
        sdc.correlation = correlation
        sdc.start_year_a = start_year_a
        sdc.start_year_b = start_year_b
        sdc.data_segment_a = segment_a
        sdc.data_segment_b = segment_b
        return sdc


def main():
    """Main function to run the SDCI extraction pipeline"""
    try:
        config = load_config()
        extractor = SDCIExtractor(config["input_path"], config["output_path"])
        indicators = config["indicators"]
        total_indicators = len(indicators)

        for index, indicator_id in enumerate(indicators, start=1):
            logger.info(
                "Processing indicator %d of %d (%.1f%% complete): %s",
                index, total_indicators,
                (index / total_indicators) * 100,
                indicator_id
            )

            sdcis = extractor.find_sdcis(
                indicator_id=indicator_id,
                min_length=config["min_length"],
                max_length=config["max_length"],
                correlation_threshold=config["correlation_threshold"]
            )

            logger.info("Found %d SDCIs for indicator %s", len(sdcis), indicator_id)

        logger.info("SDCI extraction pipeline completed successfully")

    except Exception as e:
        logger.error("SDCI extraction pipeline failed: %s", str(e))
        raise


def load_config(config_path="config.yaml"):
    """
    TODO: move to config.py
    Load configuration from YAML file
    """
    # Get the directory of the current script (app.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to config.yaml
    config_full_path = os.path.join(current_dir, config_path)

    with open(config_full_path, "r") as f:
        config = yaml.safe_load(f)
    return config


if __name__ == "__main__":
    main()