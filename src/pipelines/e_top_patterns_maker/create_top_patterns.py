"""
Optimized, Out-of-Core Top Patterns Generator (Pipeline E - Optimized Manual)

This pipeline uses an optimized, disk-based "bucket shuffle" to handle
datasets that are too large for memory. It is designed to be significantly
faster than a row-by-row pigeonhole shuffle while remaining memory-safe and
analytically correct.

**NEW in this version: Resumability for Phase 2 & 3**
- Both the slow shuffle (Phase 2) and the slow aggregation (Phase 3) are
  now fully resumable. The script can be stopped and restarted at any point
  without losing significant progress.
- The script is now resilient to temporary file corruption from interruptions.

It operates in four distinct, memory-safe phases:
1.  NORMALIZATION PASS: A fast first pass to calculate global min/max values.
2.  OPTIMIZED BUCKET SHUFFLE: A faster second pass that shuffles all rows
    into a fixed number of temporary "bucket" files on disk.
3.  GROUPED AGGREGATION: A third pass that aggregates each bucket file
    individually using a fast, in-memory groupby.
4.  FINAL PROCESSING: A final phase that combines the aggregated buckets,
    calculates the significance score, and selects the top N patterns.
"""

import os
import glob
import logging
import hashlib
import shutil
from dataclasses import dataclass
from typing import Dict, List
import yaml
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# --- Dataclasses ---
@dataclass(frozen=True)
class Config:
    """Configuration for the Optimized Manual Top Patterns pipeline."""
    input_patterns_path: str
    output_path: str
    temp_path: str
    top_n_patterns: int
    score_weights: Dict
    num_buckets: int = 512  # Number of files for the shuffle

# --- Main Pipeline Class ---
class TopPatternsOptimizedGenerator:
    """Orchestrates the optimized, out-of-core ETL process."""

    def __init__(self, config: Config):
        self.config = config
        self.indicator_registry = self._load_indicator_registry()

    def _load_indicator_registry(self) -> Dict:
        registry_path = "data/indicator_registry.yaml"
        try:
            with open(registry_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logging.error(f"[ERROR] Indicator registry not found at {registry_path}.")
            return {}

    def run(self) -> None:
        """Executes the entire manual pipeline from start to finish."""
        logging.info("[SETUP] Starting OPTIMIZED MANUAL Top Patterns Generator pipeline...")
        os.makedirs(self.config.temp_path, exist_ok=True)
        
        all_files = sorted(glob.glob(os.path.join(self.config.input_patterns_path, "*", "patterns.parquet")))
        if not all_files:
            logging.warning("[SETUP] No input pattern files found. Exiting.")
            return

        norm_params = self._calculate_global_normalization_params(all_files)
        self._optimized_bucket_shuffle(all_files)
        merged_df = self._aggregate_buckets()

        if merged_df.empty:
            logging.warning("[FINALIZE] No patterns were aggregated. Exiting.")
            self._cleanup()
            return

        logging.info("[FINALIZE] Calculating significance scores...")
        scored_df = self._calculate_significance_score(merged_df, norm_params)
        
        logging.info(f"[FINALIZE] Sorting and selecting top {self.config.top_n_patterns} patterns...")
        top_patterns_df = scored_df.sort_values("SignificanceScore", ascending=False).head(self.config.top_n_patterns)

        logging.info("[FINALIZE] Enriching final patterns with indicator names...")
        top_patterns_df['indicator_names'] = top_patterns_df['indicator_ids'].apply(
            lambda ids: [self.indicator_registry.get(id, id) for id in ids]
        )
        self._save_top_patterns(top_patterns_df)
        logging.info("[COMPLETE] Pipeline finished successfully.")

        self._cleanup()

    def _cleanup(self):
        """Deletes the temporary shuffle directory."""
        logging.info(f"[CLEANUP] Deleting temporary directory: {self.config.temp_path}")
        if os.path.exists(self.config.temp_path):
            shutil.rmtree(self.config.temp_path)

    def _calculate_global_normalization_params(self, all_files: List[str]) -> Dict:
        """PHASE 1: Iterates through all files once to find global min/max for scoring."""
        phase_log_prefix = "[PHASE 1/4]"
        logging.info(f"{phase_log_prefix} Starting Normalization Pass ({len(all_files)} files)...")
        
        global_min_length, global_max_length = np.inf, -np.inf
        global_min_corr, global_max_corr = np.inf, -np.inf
        global_min_scdi, global_max_scdi = np.inf, -np.inf

        for i, file_path in enumerate(all_files):
            progress = ((i + 1) / len(all_files)) * 100
            logging.info(f"{phase_log_prefix} Reading file {i+1}/{len(all_files)} ({progress:.1f}%) for stats...")
            try:
                df = pd.read_parquet(file_path, columns=["length", "average_correlation", "scdi_count"])
                if df.empty: continue
                
                global_min_length = min(global_min_length, df["length"].min())
                global_max_length = max(global_max_length, df["length"].max())
                global_min_corr = min(global_min_corr, df["average_correlation"].min())
                global_max_corr = max(global_max_corr, df["average_correlation"].max())
                global_min_scdi = min(global_min_scdi, df["scdi_count"].min())
                global_max_scdi = max(global_max_scdi, df["scdi_count"].max())
            except Exception as e:
                logging.error(f"[ERROR] {phase_log_prefix} Could not read stats from {file_path}. Skipping. Error: {e}")

        norm_params = {
            "length": {"min": global_min_length, "max": global_max_length},
            "average_correlation": {"min": global_min_corr, "max": global_max_corr},
            "scdi_count": {"min": global_min_scdi, "max": global_max_scdi}
        }
        logging.info(f"{phase_log_prefix} Complete. Global stats calculated.")
        return norm_params

    def _optimized_bucket_shuffle(self, all_files: List[str]) -> None:
        """PHASE 2: Shuffles all rows into a fixed number of bucket files on disk."""
        phase_log_prefix = "[PHASE 2/4]"
        logging.info(f"{phase_log_prefix} Starting Optimized Bucket Shuffle ({len(all_files)} files)...")
        
        completion_marker_path = os.path.join(self.config.temp_path, "_completed_shuffle.log")
        completed_files = set()
        if os.path.exists(completion_marker_path):
            with open(completion_marker_path, 'r') as f:
                completed_files = set(line.strip() for line in f)
            logging.info(f"{phase_log_prefix} Found {len(completed_files)} files already shuffled. Resuming...")

        try:
            for i, file_path in enumerate(all_files):
                progress = ((i + 1) / len(all_files)) * 100
                country_name = os.path.basename(os.path.dirname(file_path))

                if file_path in completed_files:
                    logging.info(f"{phase_log_prefix} Skipping already shuffled file {i+1}/{len(all_files)} ({progress:.1f}%): {country_name}")
                    continue

                logging.info(f"{phase_log_prefix} Shuffling file {i+1}/{len(all_files)} ({progress:.1f}%): {country_name}")
                
                df = pd.read_parquet(file_path)
                symmetric_df = df.rename(columns={
                    "country_a_id": "country_b_id", "country_b_id": "country_a_id",
                    "start_year_a": "start_year_b", "start_year_b": "start_year_a",
                })
                combined_df = pd.concat([df, symmetric_df], ignore_index=True)

                keys = (
                    combined_df[["country_a_id", "country_b_id"]].min(axis=1).astype(str) + "-" +
                    combined_df[["country_a_id", "country_b_id"]].max(axis=1).astype(str) + "-" +
                    combined_df["length"].astype(str) + "-" +
                    combined_df[["start_year_a", "start_year_b"]].min(axis=1).astype(str) + "-" +
                    combined_df[["start_year_a", "start_year_b"]].max(axis=1).astype(str)
                )
                hashes = keys.apply(lambda x: int(hashlib.md5(x.encode()).hexdigest(), 16))
                combined_df['bucket'] = hashes % self.config.num_buckets
                
                for bucket_id, group in combined_df.groupby('bucket'):
                    bucket_path = os.path.join(self.config.temp_path, f"bucket_{bucket_id:04d}.parquet")
                    new_table = pa.Table.from_pandas(group.drop(columns=['bucket']), preserve_index=False)
                    
                    # --- MODIFIED: Self-healing logic for corrupted bucket files ---
                    existing_table = None
                    if os.path.exists(bucket_path):
                        try:
                            if os.path.getsize(bucket_path) > 0:
                                existing_table = pq.read_table(bucket_path)
                        except pa.lib.ArrowInvalid:
                            logging.warning(f"Corrupt bucket file found: {os.path.basename(bucket_path)}. It will be overwritten.")
                            existing_table = None # Treat as if it doesn't exist

                    if existing_table is not None:
                        combined_table = pa.concat_tables([existing_table, new_table])
                        pq.write_table(combined_table, bucket_path)
                    else:
                        pq.write_table(new_table, bucket_path)

                with open(completion_marker_path, 'a') as f:
                    f.write(f"{file_path}\n")

        except Exception as e:
            logging.critical(f"[CRITICAL] {phase_log_prefix} Shuffle failed. Error: {e}", exc_info=True)
            raise

        logging.info(f"{phase_log_prefix} Complete. All data shuffled into {self.config.num_buckets} buckets.")

    def _aggregate_buckets(self) -> pd.DataFrame:
        """PHASE 3: Aggregates each bucket file and combines the results."""
        phase_log_prefix = "[PHASE 3/4]"
        bucket_files = sorted(glob.glob(os.path.join(self.config.temp_path, "bucket_*.parquet")))
        logging.info(f"{phase_log_prefix} Starting Bucket Aggregation ({len(bucket_files)} buckets)...")
        
        agg_parts_dir = os.path.join(self.config.temp_path, "aggregated_parts")
        os.makedirs(agg_parts_dir, exist_ok=True)
        completion_marker_path = os.path.join(agg_parts_dir, "_completed_aggregation.log")
        completed_buckets = set()
        if os.path.exists(completion_marker_path):
            with open(completion_marker_path, 'r') as f:
                completed_buckets = set(line.strip() for line in f)
            logging.info(f"{phase_log_prefix} Found {len(completed_buckets)} buckets already aggregated. Resuming...")

        for i, bucket_file in enumerate(bucket_files):
            progress = ((i + 1) / len(bucket_files)) * 100
            
            if bucket_file in completed_buckets:
                logging.info(f"{phase_log_prefix} Skipping already aggregated bucket {i+1}/{len(bucket_files)} ({progress:.1f}%)...")
                continue

            logging.info(f"{phase_log_prefix} Aggregating bucket {i+1}/{len(bucket_files)} ({progress:.1f}%)...")
            try:
                bucket_df = pd.read_parquet(bucket_file)
                if bucket_df.empty:
                    with open(completion_marker_path, 'a') as f:
                        f.write(f"{bucket_file}\n")
                    continue

                grouping_keys = ["country_a_id", "country_b_id", "length", "start_year_a", "start_year_b"]
                
                def custom_agg(group):
                    return pd.Series({
                        "average_correlation": group["average_correlation"].mean(),
                        "scdi_count": group["scdi_count"].sum(),
                        "indicator_ids": sorted(list(set(item for sublist in group["indicator_ids"] for item in sublist)))
                    })

                agg_df = bucket_df.groupby(grouping_keys).apply(custom_agg, include_groups=False).reset_index()
                
                part_path = os.path.join(agg_parts_dir, f"agg_part_{i:04d}.parquet")
                agg_df.to_parquet(part_path, index=False)

                with open(completion_marker_path, 'a') as f:
                    f.write(f"{bucket_file}\n")

            except Exception as e:
                logging.error(f"[ERROR] {phase_log_prefix} Failed to aggregate bucket {bucket_file}. Skipping. Error: {e}")

        logging.info(f"{phase_log_prefix} Complete. Concatenating results from all aggregated parts...")
        
        agg_part_files = glob.glob(os.path.join(agg_parts_dir, "*.parquet"))
        if not agg_part_files:
            return pd.DataFrame()
        
        all_aggregated_dfs = [pd.read_parquet(f) for f in agg_part_files]
        final_df = pd.concat(all_aggregated_dfs, ignore_index=True)

        logging.info(f"{phase_log_prefix} Performing final merge of {len(final_df):,} rows...")
        return self._aggregate_buckets_final_merge(final_df)

    def _aggregate_buckets_final_merge(self, df: pd.DataFrame) -> pd.DataFrame:
        """Performs a final, in-memory merge of aggregated results from all buckets."""
        grouping_keys = ["country_a_id", "country_b_id", "length", "start_year_a", "start_year_b"]
        
        agg_funcs = {
            'average_correlation': 'mean',
            'scdi_count': 'sum',
            'indicator_ids': lambda ids: sorted(list(set(item for sublist in ids for item in sublist)))
        }

        final_merged_df = df.groupby(grouping_keys).agg(agg_funcs).reset_index()
        logging.info(f"Final merge complete. {len(final_merged_df):,} unique patterns found.")
        return final_merged_df

    def _calculate_significance_score(self, df: pd.DataFrame, norm_params: Dict) -> pd.DataFrame:
        """PHASE 4: Calculates the weighted significance score for each pattern."""
        phase_log_prefix = "[PHASE 4/4]"
        logging.info(f"{phase_log_prefix} Calculating significance scores...")
        weights = self.config.score_weights
        
        df["SignificanceScore"] = (
            weights["length"] * self._min_max_scale(df["length"], norm_params["length"]) +
            weights["average_correlation"] * self._min_max_scale(df["average_correlation"], norm_params["average_correlation"]) +
            weights["scdi_count"] * self._min_max_scale(df["scdi_count"], norm_params["scdi_count"])
        )
        return df

    def _min_max_scale(self, series: pd.Series, params: Dict) -> pd.Series:
        """Applies min-max scaling to a pandas Series."""
        min_val, max_val = params["min"], params["max"]
        if max_val == min_val: return 0.5
        return (series - min_val) / (max_val - min_val)

    def _save_top_patterns(self, df: pd.DataFrame) -> str:
        """Saves the final DataFrame to a single Parquet file."""
        os.makedirs(self.config.output_path, exist_ok=True)
        output_file_path = os.path.join(self.config.output_path, "top_patterns.parquet")
        logging.info(f"[FINALIZE] Saving {len(df)} top patterns to {output_file_path}...")
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, output_file_path, compression="brotli")
        return output_file_path


def load_config(config_path: str = "config.yaml") -> Config:
    """Loads the YAML configuration file and returns a validated Config object."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_full_path = os.path.join(script_dir, config_path)
    logging.info(f"[SETUP] Loading configuration from: {config_full_path}")
    with open(config_full_path, "r") as f:
        config_dict = yaml.safe_load(f)
    return Config(**config_dict)


if __name__ == "__main__":
    try:
        config = load_config()
        pipeline = TopPatternsOptimizedGenerator(config)
        pipeline.run()
    except KeyboardInterrupt:
        logging.warning("\n[INTERRUPTED] Pipeline stopped by user. Progress in Phase 2 or 3 has been saved.")
    except Exception as e:
        logging.critical(f"[CRITICAL] Pipeline failed with an unhandled exception: {e}", exc_info=True)
