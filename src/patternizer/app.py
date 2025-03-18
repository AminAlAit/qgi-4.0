"""SDCI Extraction Application"""

import logging
import os
import yaml
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import pyarrow.parquet as pq
import json
from datetime import datetime
import mysql.connector
from mysql.connector import Error


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")


class SDCI:
    """Class representing a Single Dyadic Correlating Index"""

    def __init__(self):
        self.country_a_id = 0
        self.country_b_id = 0
        self.length = 0
        self.start_year_a = 0
        self.start_year_b = 0
        self.correlation = 0.0


class SDCIExtractor:
    """Class for extracting SDCIs from indicator data"""

    def __init__(self, data_path, output_path):
        self.data_path = data_path
        self.output_path = output_path
        self.processing_log_path = os.path.join(data_path, "processing_log.json")
        
        # Load countries.csv to create name-to-id mapping
        countries_csv_path = r"/workspace/qgi\qgi-4.0\data\countries.csv"
        self.countries_df = pd.read_csv(countries_csv_path)
        self.country_name_to_id = {row["name_1"]: row["id"] for _, row in self.countries_df.iterrows()}

        # MySQL connection configuration
        self.mysql_config = {
            'host': 'localhost',
            'user': 'amin',
            'password': '<MYSQL_PASSWORD>',
            'database': 'sdci_database'
        }

    def load_indicator_data(self, indicator_id):
        """Load cleaned indicator data from Parquet file"""
        try:
            indicator_dir = os.path.join(self.data_path, indicator_id)
            parquet_path = os.path.join(indicator_dir, f"{indicator_id}.parquet")
            table = pq.read_table(parquet_path)
            df = table.to_pandas()
            return df
        except Exception as e:
            logger.error(f"Error loading data for {indicator_id}: {str(e)}")
            raise

    def find_sdcis(self, indicator_id, min_length=3, correlation_threshold=0.9):
        """Main method to find SDCIs in the indicator data"""
        df = self.load_indicator_data(indicator_id)
        countries = df["country"].unique()

        # Preprocess data for each country
        preprocessed_data = self._preprocess_data(df, countries)

        # Calculate total number of comparisons for progress tracking
        total_comparisons = self._calculate_total_comparisons(
            preprocessed_data, min_length
        )

        # Find SDCIs with progress tracking
        sdcis = self._find_sdcis(
            indicator_id,
            preprocessed_data,
            total_comparisons,
            min_length,
            correlation_threshold
        )

        return sdcis

    def _preprocess_data(self, df, countries):
        """
        Preprocesses data for each country by handling NaN values and organizing data for analysis.

        This function processes the input DataFrame to prepare data for each country:
        1. Filters the data for each country
        2. Sorts the data by year
        3. Handles NaN values by either skipping countries with more than one NaN or interpolating single NaN values
        4. Creates a list of dictionaries containing the country name, original data, and cleaned values

        Parameters:
        df (pd.DataFrame): The input DataFrame containing indicator data for multiple countries
        countries (list): List of country names to process

        Returns:
        list: A list of dictionaries, where each dictionary contains:
            - "country": The country name
            - "data": The original country data sorted by year
            - "values": Cleaned numeric values with NaNs handled
        """
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

    def _calculate_total_comparisons(self, preprocessed_data, min_length):
        """
        Calculate the total number of segment comparisons for progress tracking.

        This function computes the total number of comparisons that will be made 
        during the pattern extraction process. It considers all possible segment 
        lengths within the specified range and all country pairs.

        The calculation is based on:
        1. For each country A, considering all valid segment lengths
        2. For each segment length, determining how many segments can be created
        3. For each segment of country A, counting how many valid segments exist 
        in all subsequent countries (B) that can be compared with it

        Parameters:
        preprocessed_data (list): List of dictionaries containing preprocessed data for each country
        min_length (int): Minimum segment length to consider

        Returns:
        int: Total number of comparisons that will be performed
        """
        total_comparisons = 0

        for i, country_a in enumerate(preprocessed_data):
            values_a = country_a["values"]
            max_length_a = len(values_a) - 1

            for segment_length in range(min_length, max_length_a + 1):
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
        correlation_threshold
    ):
        """
        Identify Single Dyadic Correlating Indexes (SDCIs) between country pairs.

        This method performs the core pattern identification by:
        1. Iterating over all country pairs
        2. Generating all possible segments of specified lengths
        3. Calculating correlations between segments
        4. Saving qualifying SDCIs to disk incrementally
        5. Tracking and logging progress throughout the process

        The method resumes from previously identified SDCIs if they exist and
        processes only new country pairs.

        Parameters:
        indicator_id (str): The ID of the indicator being processed
        preprocessed_data (list): List of dictionaries containing preprocessed data for each country
        total_comparisons (int): Total number of comparisons to be made for progress tracking
        min_length (int): Minimum segment length to consider
        correlation_threshold (float): Minimum correlation required to qualify as an SDCI

        Returns:
        dict: A dictionary representation of all identified SDCIs
        """
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
            existing_countries = set(sdcis_df["country_a_id"].unique())
            logger.info("Resuming from existing SDCIs for %s", indicator_id)

        # Filter out countries already processed
        preprocessed_data = [
            country for country in preprocessed_data
            if country["country"] not in existing_countries
        ]

        ## TODO: give each loop a short one line description
        for i, country_a in enumerate(preprocessed_data):
            values_a = country_a["values"]
            country_a_name = country_a["country"]
            country_a_sdcis = []

            max_length_a = len(values_a) - 1

            # Calculate total comparisons for this country
            total_comparisons_country = 0
            for segment_length in range(min_length, max_length_a + 1):
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

            for segment_length in range(min_length, max_length_a + 1):
                if len(values_a) < segment_length:
                    continue

                num_segments_a = len(values_a) - segment_length + 1

                for start_idx in range(num_segments_a):
                    segment_a = values_a[start_idx:start_idx + segment_length]

                    for country_b in preprocessed_data[i+1:]:
                        values_b = country_b["values"]
                        country_b_name = country_b["country"]

                        max_length_b = len(values_b) - 1
                        if segment_length > max_length_b:
                            continue

                        num_segments_b = len(values_b) - segment_length + 1

                        for start_idx_b in range(num_segments_b):
                            segment_b = values_b[start_idx_b:start_idx_b + segment_length]

                            # Calculate correlation
                            correlation, _ = pearsonr(segment_a, segment_b)

                            if correlation >= correlation_threshold:
                                sdc = SDCI()
                                # Convert country names to ids using the mapping
                                sdc.country_a_id = self.country_name_to_id.get(country_a_name, 0)
                                sdc.country_b_id = self.country_name_to_id.get(country_b_name, 0)
                                sdc.length = segment_length
                                sdc.correlation = correlation
                                sdc.start_year_a = country_a["data"].iloc[start_idx]["year"]
                                sdc.start_year_b = country_b["data"].iloc[start_idx_b]["year"]
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
                logger.info("Saving SDCIs for country %s to Parquet", country_a_name)
                self._save_to_parquet(country_a_sdcis, sdcis_file, sdcis_df)

                logger.info("Saving SDCIs for country %s to MySQL", country_a_name)
                self._save_to_mysql(country_a_sdcis, indicator_id)

                sdcis_df = pd.concat([sdcis_df, self._convert_sdcis_to_df(country_a_sdcis)], ignore_index=True)
                sdcis.extend(country_a_sdcis)

        return sdcis

    def _convert_sdcis_to_df(self, sdcis):
        """Convert list of SDCI objects to DataFrame"""
        sdcis_df = pd.DataFrame([
            {
                "country_a_id": sdc.country_a_id,
                "country_b_id": sdc.country_b_id,
                "length":       sdc.length,
                "start_year_a": sdc.start_year_a,
                "start_year_b": sdc.start_year_b,
                "correlation":  round(sdc.correlation, 6)
            } for sdc in sdcis
        ])

        # Convert to optimal data types
        sdcis_df = sdcis_df.astype({
            "country_a_id": "uint16",
            "country_b_id": "uint16",
            "length":       "uint16",
            "start_year_a": "uint16",
            "start_year_b": "uint16",
            "correlation":  "float32"
        })

        return sdcis_df

    def _save_to_parquet(self, country_a_sdcis, sdcis_file, sdcis_df):
        """Save SDCIs to Parquet file"""
        country_a_sdcis_df = self._convert_sdcis_to_df(country_a_sdcis)

        combined_df = pd.concat([sdcis_df, country_a_sdcis_df], ignore_index=True)
        combined_df.to_parquet(
            sdcis_file,
            index=False,
            compression="gzip",
            coerce_timestamps="ms",
            allow_truncated_timestamps=True
        )

    def _save_to_mysql(self, country_a_sdcis, indicator_id):
        """Save SDCIs to MySQL database"""
        sdcis_df = self._convert_sdcis_to_df(country_a_sdcis)
        sdcis_df["indicator_id"] = indicator_id  # Add indicator_id column

        try:
            connection = mysql.connector.connect(**self.mysql_config)
            if connection.is_connected():
                cursor = connection.cursor()

                # Prepare INSERT query
                insert_query = """
                INSERT INTO sdcis 
                (country_a_id, country_b_id, length, start_year_a, start_year_b, correlation, indicator_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                # Convert DataFrame to list of tuples for batch insert
                data_to_insert = list(sdcis_df.itertuples(index=False, name=None))

                # Execute batch insert
                cursor.executemany(insert_query, data_to_insert)
                connection.commit()
                logger.info("Inserted %d SDCIs into MySQL", len(data_to_insert))

        except Error as e:
            logger.error("Error saving to MySQL: %s", e)
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def update_processing_log(
        self, indicator_id, status, num_sdcis=0, error_message=None, last_year=None, correlation_threshold=None
    ):
        """Update the processing log with status for an indicator"""
        try:
            # Load existing log or create new
            if os.path.exists(self.processing_log_path):
                try:
                    with open(self.processing_log_path, 'r') as f:
                        log = json.load(f)
                except json.JSONDecodeError:
                    # If JSON is invalid, start with an empty log
                    log = {}
                    logger.warning("Processing log was corrupted. Starting with a new log.")
            else:
                log = {}

            # Ensure last_year is a standard integer
            if last_year is not None:
                last_year = int(last_year)

            # Update indicator entry
            log[indicator_id] = {
                "status": status,
                "num_sdcis": num_sdcis,
                "error_message": error_message,
                "last_updated": datetime.now().isoformat(),
                "last_year": last_year,
                "correlation_threshold": correlation_threshold
            }

            # Save updated log
            with open(self.processing_log_path, 'w') as f:
                json.dump(log, f, indent=2)

        except Exception as e:
            logger.error(f"Error updating processing log: {str(e)}")


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

            # Load indicator data to get last year
            df = extractor.load_indicator_data(indicator_id)
            last_year = df["year"].max()

            # Update processing log to "in progress"
            extractor.update_processing_log(
                indicator_id,
                "in progress", 
                last_year=last_year,
                correlation_threshold=config["correlation_threshold"]
            )

            try:
                sdcis = extractor.find_sdcis(
                    indicator_id=indicator_id,
                    min_length=config["min_length"],
                    #max_length=config["max_length"],
                    correlation_threshold=config["correlation_threshold"]
                )

                # Update processing log to "completed"
                extractor.update_processing_log(
                    indicator_id,
                    "completed",
                    num_sdcis=len(sdcis),
                    last_year=last_year,
                    correlation_threshold=config["correlation_threshold"]
                )

                logger.info("Found %d SDCIs for indicator %s", len(sdcis), indicator_id)

            except Exception as e:
                # Update processing log to "failed"
                extractor.update_processing_log(
                    indicator_id,
                    "failed",
                    error_message=str(e),
                    last_year=last_year,
                    correlation_threshold=config["correlation_threshold"]
                )
                logger.error("Processing failed for indicator %s: %s", indicator_id, str(e))
                continue

        logger.info("SDCI extraction pipeline completed successfully")

    except Exception as e:
        logger.error("SDCI extraction pipeline failed: %s", str(e))
        raise


def load_config(config_path="config.yaml"):
    """
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
