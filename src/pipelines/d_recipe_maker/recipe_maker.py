"""
Recipe Maker Pipeline

This script processes Similar Country Development Indicators (SCDIs),
identifies correlations with historical events, and generates "recipes"
that link indicators to event types.
"""

import os
import re
import glob
import logging
from dataclasses import dataclass
import pandas as pd
import yaml


@dataclass(frozen=True)
class RecipeMakerConfig:
    """Validated configuration for the RecipeMaker pipeline."""
    input_scdi_path: str
    output_recipes_path: str
    events_path: str
    countries_csv_path: str


@dataclass
class Event:
    """Represents a single historical event."""
    year: int
    event_type: str
    description: str


class RecipeMaker:
    """Generates recipes by correlating SCDIs with historical events."""

    def __init__(self, config: RecipeMakerConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.countries_df = pd.read_csv(config.countries_csv_path)
        self.id_to_name_map = dict(zip(self.countries_df['id'], self.countries_df['name_1']))
        self.events_data: dict[str, list[Event]] = {}

    def run(self):
        """Executes the full recipe generation pipeline."""
        self.logger.info("Starting Recipe Maker pipeline...")
        
        self._load_events()
        if not self.events_data:
            self.logger.error("No events data loaded. Pipeline cannot continue.")
            return

        all_recipes = self._process_all_scdis()
        
        self.logger.info(f"Aggregated {len(all_recipes)} event types. Now saving to files...")
        for event_type, recipes in all_recipes.items():
            recipe_df = pd.DataFrame(recipes)
            self._save_or_merge_recipe(recipe_df, event_type)
        
        self.logger.info("Recipe Maker pipeline completed successfully.")

    def _load_events(self):
        """Loads all event files from the configured path."""
        self.logger.info(f"Loading events from: {self.config.events_path}")
        if not os.path.exists(self.config.events_path):
            self.logger.warning("Events path not found.")
            return

        for filename in os.listdir(self.config.events_path):
            if not filename.endswith(".txt"):
                continue
            
            country_name = filename.replace(".txt", "")
            file_path = os.path.join(self.config.events_path, filename)
            country_events = []
            
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(" | ")
                    if len(parts) == 4:
                        try:
                            country_events.append(Event(
                                year=int(parts[1]),
                                event_type=parts[2].strip(),
                                description=parts[3].strip()
                            ))
                        except ValueError:
                            continue # Skip lines with non-integer years
            
            if country_events:
                self.events_data[country_name] = country_events
        self.logger.info(f"Loaded events for {len(self.events_data)} countries.")

    def _process_all_scdis(self) -> dict:
        """Iterates through all SCDI files and generates recipe references."""
        scdi_files = glob.glob(os.path.join(self.config.input_scdi_path, "*", "*.parquet"))
        self.logger.info(f"Found {len(scdi_files)} SCDI files to process.")
        
        # This will hold all found recipes in memory before writing to disk
        accumulated_recipes = {}

        for i, file_path in enumerate(scdi_files):
            self.logger.info(f"Processing file {i+1}/{len(scdi_files)}: {file_path}")
            df = pd.read_parquet(file_path)
            indicator_name = os.path.basename(os.path.dirname(file_path))
            country_file_name = os.path.splitext(os.path.basename(file_path))[0]

            for index, row in df.iterrows():
                fitting_events = self._get_fitting_events_for_scdi(row)
                for event_type in fitting_events:
                    recipe_ref = {
                        "indicator_name": indicator_name,
                        "country_name": country_file_name,
                        "scdi_row_index": index,
                        "correlation": row.get("correlation"),
                    }
                    accumulated_recipes.setdefault(event_type, []).append(recipe_ref)
        
        return accumulated_recipes

    def _get_fitting_events_for_scdi(self, scdi_row: pd.Series) -> list[str]:
        """Finds matching event types for a single SCDI row."""
        country_a_name = self.id_to_name_map.get(scdi_row.get("country_a_id"))
        country_b_name = self.id_to_name_map.get(scdi_row.get("country_b_id"))
        
        if not country_a_name or not country_b_name:
            return []

        end_year_a = int(scdi_row.get("start_year_a") + scdi_row.get("length"))
        end_year_b = int(scdi_row.get("start_year_b") + scdi_row.get("length"))

        country_a_events = self.events_data.get(country_a_name, [])
        country_b_events = self.events_data.get(country_b_name, [])
        
        fitting_event_types = set()
        for event_a in country_a_events:
            if event_a.year == end_year_a:
                for event_b in country_b_events:
                    if event_b.year == end_year_b and event_a.event_type == event_b.event_type:
                        fitting_event_types.add(event_a.event_type)
        return list(fitting_event_types)

    def _save_or_merge_recipe(self, new_recipe_df: pd.DataFrame, event_type: str):
        """Saves or merges a recipe DataFrame to a Parquet file."""
        sanitized_name = re.sub(r'(?u)[^-\w.]', '', event_type.strip().replace(" ", "_")).lower()
        output_file = os.path.join(self.config.output_recipes_path, f"{sanitized_name}.parquet")
        
        os.makedirs(self.config.output_recipes_path, exist_ok=True)

        if os.path.exists(output_file):
            self.logger.info(f"Merging with existing recipe: {event_type}")
            existing_df = pd.read_parquet(output_file)
            combined_df = pd.concat([existing_df, new_recipe_df], ignore_index=True)
            # Drop duplicates based on the reference keys
            final_df = combined_df.drop_duplicates(
                subset=["indicator_name", "country_name", "scdi_row_index"], keep="last"
            )
        else:
            final_df = new_recipe_df
        
        final_df.to_parquet(output_file, index=False, compression='brotli')
        self.logger.info(f"Saved recipe for '{event_type}' with {len(final_df)} entries.")


def load_config(config_path="config.yaml") -> RecipeMakerConfig:
    """Loads YAML config file into a dataclass object."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_full_path = os.path.join(current_dir, config_path)
    with open(config_full_path, "r") as f:
        config_dict = yaml.safe_load(f)
    return RecipeMakerConfig(**config_dict)

def main():
    """Main function to initialize and run the Recipe Maker pipeline."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    try:
        config = load_config()
        pipeline = RecipeMaker(config)
        pipeline.run()
    except Exception:
        logging.getLogger(__name__).critical("Pipeline failed with an unhandled exception.", exc_info=True)
        raise

if __name__ == "__main__":
    main()
