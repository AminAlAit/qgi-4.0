"""TODO"""

import os
import re
import pandas as pd
import json


class EventRecipeGenerator:
    """
    A class to process Similar Country Development Indicators (SCDIs),
    identify correlations with historical events, and generate "recipes"
    that link indicators to event types. It also provides functionality
    to analyze these generated recipes.
    """

    def __init__(
        self,
        base_data_path=r"/workspace/qgi\qgi-4.0\data",
        scdi_subfolder="scdi_finder_output",
        events_subfolder="events",
        countries_csv_name="countries.csv",
        recipes_subfolder="recipes"
    ):
        """
        Initializes the EventRecipeGenerator with necessary paths and data.

        Args:
            base_data_path (str): The root directory for all data files.
            scdi_subfolder (str): Subfolder name for SCDI Parquet files.
            events_subfolder (str): Subfolder name for event text files.
            countries_csv_name (str): Filename for the countries CSV.
            recipes_subfolder (str): Subfolder name for outputting recipe Parquet files.
        """

        self.BASE_DATA_PATH = base_data_path
        self.SCDIS_PATH = os.path.join(self.BASE_DATA_PATH, scdi_subfolder)
        self.EVENTS_PATH = os.path.join(self.BASE_DATA_PATH, events_subfolder)
        self.COUNTRIES_CSV_PATH = os.path.join(self.BASE_DATA_PATH, countries_csv_name)
        self.RECIPES_PATH = os.path.join(self.BASE_DATA_PATH, recipes_subfolder)

        os.makedirs(self.RECIPES_PATH, exist_ok=True)

        self.STATE_FILE_PATH = r"/workspace/qgi\qgi-4.0\src\recipe_maker\recipe_generation_state.json"

        try:
            self.countries_df = pd.read_csv(self.COUNTRIES_CSV_PATH)
        except FileNotFoundError:
            print(f"CRITICAL Error: countries.csv not found at {self.COUNTRIES_CSV_PATH}. Some functionalities will fail.")
            self.countries_df = pd.DataFrame() # Allow instantiation but flag issue

        self.events_data = None # Loaded separately by load_events()

    def _sanitize_filename(self, filename, lower=False):
        """
        Sanitizes a string to be used as a valid filename.

        Args:
            filename (str): The string to sanitize.
            lower (bool): Whether to convert the filename to lowercase.

        Returns:
            str: The sanitized filename.
        """
        s = str(filename).strip().replace(" ", "_")
        s = re.sub(r"(?u)[^-\w.]", "", s)  # Remove characters not alphanumeric, underscore, hyphen, or dot
        if not s:
            s = "_"
        return s.lower() if lower else s

    def _load_processing_state(self):
        """Loads the processing state from the JSON file."""
        if os.path.exists(self.STATE_FILE_PATH):
            try:
                with open(self.STATE_FILE_PATH, 'r') as f:
                    state = json.load(f)
                    # Ensure all keys are present
                    state.setdefault("completed_indicator_folders", [])
                    state.setdefault("processed_parquet_files", {})
                    state.setdefault("last_run_completed_fully", False)
                    return state
            except json.JSONDecodeError:
                print(f"Warning: State file {self.STATE_FILE_PATH} is corrupted. Resetting state.")
                return self._reset_processing_state() # Return a fresh state
        return self._reset_processing_state() # If file doesn't exist, start fresh

    def _save_processing_state(self, state_data):
        """Saves the processing state to the JSON file."""
        try:
            with open(self.STATE_FILE_PATH, 'w') as f:
                json.dump(state_data, f, indent=4)
        except IOError as e:
            print(f"Error saving processing state to {self.STATE_FILE_PATH}: {e}")

    def _reset_processing_state(self):
        """Resets the processing state to its initial empty state and saves it."""
        print("Resetting recipe generation processing state.")
        initial_state = {
            "completed_indicator_folders": [],
            "processed_parquet_files": {},
            "last_run_completed_fully": False
        }
        self._save_processing_state(initial_state)
        return initial_state

    def load_events(self, events_path=None):
        """
        Loads events from text files into self.events_data.

        Args:
            events_path (str, optional): Path to the directory containing event text files.
                                         Defaults to self.EVENTS_PATH.
        """
        current_events_path = events_path if events_path else self.EVENTS_PATH
        self.events_data = {}
        if not os.path.exists(current_events_path):
            print(f"Warning: Events path not found: {current_events_path}")
            return

        print(f"Loading events from: {current_events_path}...")
        for file in os.listdir(current_events_path):
            if file.endswith('.txt'):
                country_name = file.replace('.txt', '')
                file_path = os.path.join(current_events_path, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                except Exception as e:
                    print(f"Error reading event file {file_path}: {e}")
                    continue

                country_events = []
                for line_num, line_content in enumerate(lines):
                    line_content = line_content.strip()
                    if line_content.startswith('. '):
                        line_content = line_content[2:]
                        parts = line_content.split(' | ')
                        if len(parts) == 4:
                            _discarded_country_name, year_str, event_type, description = parts
                            try:
                                country_events.append({
                                    'year': int(year_str),
                                    'event_type': event_type.strip(),
                                    'description': description.strip()
                                })
                            except ValueError:
                                print(f"Warning: Could not parse year '{year_str}' in {file_path}, line {line_num + 1}")
                        # else:
                        #     print(f"Warning: Line format incorrect in {file_path}, line {line_num + 1}: {line_content}")

                if country_events:
                    self.events_data[country_name] = country_events
        print(f"Loaded events for {len(self.events_data)} countries.")

    def _get_name_by_id(self, input_id):
        """Helper to get country name from ID using self.countries_df."""
        if self.countries_df.empty:
            return None
        try:
            id_int = int(float(input_id)) # Handle cases like 2.0 from Parquet
            result = self.countries_df[self.countries_df["id"] == id_int]
            if not result.empty:
                return result["name_1"].iloc[0]
        except ValueError:
            pass # Invalid ID format
        return None

    def _get_id_by_name(self, input_name):
        """Helper to get country ID from name using self.countries_df."""
        if self.countries_df.empty:
            # print("Warning: Countries DataFrame is empty. Cannot get ID by name.") # Covered by __init__ warning
            return None
        if "id" not in self.countries_df.columns:
            # print("Warning: Column 'id' not found in countries_df.") # Covered by __init__ warning (implicitly)
            return None
        if not isinstance(input_name, str) or not input_name.strip():
            return None

        normalized_input_name = input_name.lower().strip()
        name_columns_to_search = ['name_1'] + [f'name_{i}' for i in range(2, 21)]

        for col_name in name_columns_to_search:
            if col_name in self.countries_df.columns:
                matches = self.countries_df[self.countries_df[col_name].astype(str).str.lower().str.strip() == normalized_input_name]
                if not matches.empty:
                    return matches["id"].iloc[0]
        return None

    def _get_fitting_events_for_scdi(self, scdi_row):
        """
        Returns event types that fit the given SCDI row based on self.events_data.
        The logic for end year is start_year + length.
        """
        if not self.events_data:
            print("Error: Events data not loaded. Call load_events() first.")
            return []

        country_a_id = scdi_row.get("country_a_id")
        country_b_id = scdi_row.get("country_b_id")
        length = scdi_row.get("length")
        start_year_a_val = scdi_row.get("start_year_a")
        start_year_b_val = scdi_row.get("start_year_b")

        if any(v is None for v in [country_a_id, country_b_id, length, start_year_a_val, start_year_b_val]):
            return []

        country_a_name = self._get_name_by_id(country_a_id)
        country_b_name = self._get_name_by_id(country_b_id)

        if not country_a_name or not country_b_name:
            return []

        # TODO for end_year: current logic is (start_year + length).
        # Original notes mentioned potential for -1 and margins like [-2, +2].
        # This could be parameterized in the future if needed.
        end_year_a = int(start_year_a_val + length)
        end_year_b = int(start_year_b_val + length)

        country_a_events_list = self.events_data.get(str(country_a_name), [])
        country_b_events_list = self.events_data.get(str(country_b_name), [])

        fitting_event_types = set()
        for event_a in country_a_events_list:
            if event_a["year"] == end_year_a:
                for event_b in country_b_events_list:
                    if event_b["year"] == end_year_b and event_b["event_type"] == event_a["event_type"]:
                        fitting_event_types.add(event_a["event_type"])
        return list(fitting_event_types)

    def _save_or_merge_recipe_data(self, new_recipe_df, event_type_str, base_recipes_path, do_print_progress=True):
        """
        Saves a DataFrame to a Parquet file for a given event type.
        If the file already exists, it merges the new data with the existing data,
        removing duplicates and ensuring consistent string typing for key columns.
        """
        if new_recipe_df.empty:
            if do_print_progress:
                print(f"  No new data to save for event: {event_type_str}")
            return

        sanitized_name = self._sanitize_filename(event_type_str, lower=True)
        output_file_path = os.path.join(base_recipes_path, f"{sanitized_name}.parquet")
        
        key_columns = ['indicator_name', 'country_name', 'scdi_row_index']

        # Ensure consistent types in the new data before any operation
        if 'country_name' in new_recipe_df.columns:
            new_recipe_df['country_name'] = new_recipe_df['country_name'].astype(str)
        if 'indicator_name' in new_recipe_df.columns:
            new_recipe_df['indicator_name'] = new_recipe_df['indicator_name'].astype(str)
        # scdi_row_index should be integer, correlation float - ensure they are if issues arise.

        try:
            if os.path.exists(output_file_path):
                if do_print_progress:
                    print(f"  Merging with existing recipe: '{os.path.join(os.path.basename(os.path.dirname(output_file_path)), os.path.basename(output_file_path))}'...")
                
                existing_df = pd.read_parquet(output_file_path)
                
                # Ensure consistent types in the existing data as well
                if 'country_name' in existing_df.columns:
                    existing_df['country_name'] = existing_df['country_name'].astype(str)
                if 'indicator_name' in existing_df.columns:
                    existing_df['indicator_name'] = existing_df['indicator_name'].astype(str)

                # Now, both DataFrames should have 'country_name' as string, ready for concat
                combined_df = pd.concat([existing_df, new_recipe_df], ignore_index=True)
                
                rows_before_dedup = len(combined_df)
                # drop_duplicates should now work correctly with consistent types
                final_df = combined_df.drop_duplicates(subset=key_columns, keep='first')
                rows_after_dedup = len(final_df)
                
                final_df.to_parquet(output_file_path, index=False)
                if do_print_progress:
                    print(f"    Merged recipe saved. Path: '{output_file_path}'")
                    print(f"    Total entries: {rows_after_dedup} (added {rows_after_dedup - len(existing_df)}, duplicates removed: {rows_before_dedup - rows_after_dedup}).")
            else:
                # New file, types in new_recipe_df are already enforced
                new_recipe_df.to_parquet(output_file_path, index=False)
                if do_print_progress:
                    print(f"  New recipe saved: {output_file_path} with {len(new_recipe_df)} entries.")
        except Exception as e:
            if do_print_progress:
                # Print a more detailed error, including the type
                print(f"  Error processing recipe file {output_file_path} (Event Type: {event_type_str}): {type(e).__name__} - {e}")

    def _save_accumulated_recipes_snapshot(self, current_event_recipes_accumulator, print_progress_flag):
        """
        Helper function to save/merge the currently accumulated recipes.
        This is intended to be called after processing each SCDI parquet file,
        acting on the full set of recipes collected so far in the run.

        Args:
            current_event_recipes_accumulator (dict): The dictionary holding all event recipes
                                                      accumulated so far in the current run.
            print_progress_flag (bool): Flag to control progress printing.
        """
        # The following print statements are based on the user's provided block.
        # Their context shifts slightly as this now runs per SCDI Parquet file.
        if print_progress_flag:
            # This message now indicates the end of processing for the *current file* before saving.
            print("\nFinished processing current SCDI parquet file. Proceeding to save/merge accumulated recipes.") 
        
        if not current_event_recipes_accumulator:
            if print_progress_flag:
                print("  No recipes accumulated in this run so far to save/merge.")
        else:
            if print_progress_flag:
                print(f"  Saving/Merging {len(current_event_recipes_accumulator)} event type(s) (cumulative) to '{self.RECIPES_PATH}'...")
            for event_type, scdi_refs_list in current_event_recipes_accumulator.items():
                # Create a DataFrame from the list of references for the current event_type
                # This list contains all references found for this event_type so far in the entire run.
                cumulative_df_for_event_type = pd.DataFrame(scdi_refs_list)
                
                # Call the existing detailed save/merge logic.
                # _save_or_merge_recipe_data handles the actual file IO and deduplication.
                self._save_or_merge_recipe_data(
                    new_recipe_df=cumulative_df_for_event_type,
                    event_type_str=event_type,
                    base_recipes_path=self.RECIPES_PATH,
                    do_print_progress=print_progress_flag
                )

        if print_progress_flag:
            # This message now confirms completion for the current snapshot.
            print("  Cumulative recipe snapshot saving/merging operations complete for this stage.")

    def generate_recipes(self, print_progress=True, force_reset_state=False):
        """
        Processes all SCDIs, finds fitting events, and saves SCDI references
        to recipe Parquet files in self.RECIPES_PATH.
        """
        if self.events_data is None:
            print("Events data not loaded. Attempting to load now...")
            self.load_events()
            if self.events_data is None:
                print("CRITICAL Error: Events data could not be loaded. Cannot generate recipes.")
                return
        
        if self.countries_df.empty:
            print("CRITICAL Error: Countries DataFrame is empty. Recipe generation might be unreliable.")
            # Decide if to proceed or return based on severity

        countries_with_events = list(self.events_data.keys())

        state = self._load_processing_state()
        if force_reset_state or state.get("last_run_completed_fully", False):
            if print_progress:
                action_message = "Forced reset" if force_reset_state else "Previous run completed fully, resetting"
                print(f"{action_message} of processing state for recipe generation.")
            state = self._reset_processing_state() # Resets and reloads the empty state

        state["last_run_completed_fully"] = False # Mark current run as not completed yet
        # Do an initial save in case of interruption before any processing
        self._save_processing_state(state)

        print("Starting to browse SCDIs and generate recipes...")

        if not os.path.exists(self.SCDIS_PATH):
            print(f"Error: SCDIs base path not found: {self.SCDIS_PATH}")
            return

        indicator_folders = [d for d in os.listdir(self.SCDIS_PATH) if os.path.isdir(os.path.join(self.SCDIS_PATH, d))]
        total_indicator_folders = len(indicator_folders)

        if total_indicator_folders == 0:
            print(f"No indicator folders found in {self.SCDIS_PATH}")
            return

        for i, indicator_folder_name in enumerate(indicator_folders):
            if indicator_folder_name in state.get("completed_indicator_folders", []):
                if print_progress:
                    # Ensure this print is not immediately overwritten by end='\r'
                    print(f"\nSkipping already completed indicator folder: {indicator_folder_name} ({(i + 1) / total_indicator_folders * 100:.1f}%)")
                continue
            # Ensure dict entry exists for current indicator for its parquet files
            state.get("processed_parquet_files", {}).setdefault(indicator_folder_name, [])

            indicator_path = os.path.join(self.SCDIS_PATH, indicator_folder_name)
            country_files = [f for f in os.listdir(indicator_path) if f.endswith(".parquet")]
            total_country_files = len(country_files)

            if total_country_files == 0 and print_progress:
                print(f"No Parquet files found in indicator folder: {indicator_folder_name}")
                continue

            all_event_recipes_data_for_run = {} # {event_type: [list_of_scdi_refs]}
            for j, scdi_file_name in enumerate(country_files):
                # Check if this specific parquet file in this indicator folder is already processed
                if scdi_file_name in state.get("processed_parquet_files", {}).get(indicator_folder_name, []):
                    if print_progress:
                        # Ensure this print is not immediately overwritten by end='\r'
                        print(f"\nSkipping: {indicator_folder_name} > {scdi_file_name} (already processed).")
                    continue

                parquet_file_country_name = scdi_file_name.replace(".parquet", "") # This is the country name from the filename

                if parquet_file_country_name not in countries_with_events:
                    print(f"\nSkipping: {indicator_folder_name} > {scdi_file_name} (does not have events).")
                    continue # skip countries that don't have events

                file_path = os.path.join(indicator_path, scdi_file_name)

                try:
                    scdis_df = pd.read_parquet(file_path)
                except Exception as e:
                    if print_progress: print(f"Error reading Parquet file {file_path}: {e}")
                    continue

                total_rows = len(scdis_df)
                if total_rows == 0:
                    continue

                for k, scdi_row in scdis_df.iterrows():
                    if print_progress:
                        indicator_percent = (i + 1) / total_indicator_folders * 100
                        country_percent = (j + 1) / total_country_files * 100
                        row_percent = (k + 1) / total_rows * 100

                        progress_print_message = (
                            f"{indicator_folder_name} ({indicator_percent:.1f}%) | "
                            f"{parquet_file_country_name} ({country_percent:.1f}%) | "
                            f"Row {k} ({row_percent:.1f}%)"
                        )

                    fitting_event_types = self._get_fitting_events_for_scdi(scdi_row)

                    if fitting_event_types:
                        if print_progress:
                            print()
                            row_country_b_name_disp = self._get_name_by_id(scdi_row.get('country_b_id')) or "N/A"
                            progress_print_message = (
                                progress_print_message + f" | {fitting_event_types} | {row_country_b_name_disp}"
                            )

                        for event_type in fitting_event_types:
                            scdi_reference = {
                                'indicator_name': indicator_folder_name,
                                'country_name': parquet_file_country_name,
                                'scdi_row_index': k,
                                'correlation': scdi_row.get('correlation', None)
                            }
                            if event_type not in all_event_recipes_data_for_run:
                                all_event_recipes_data_for_run[event_type] = []
                            all_event_recipes_data_for_run[event_type].append(scdi_reference)
                    
                    print(progress_print_message , end = "\r")
                    # break # 1 row
                # break # country parquet file
            # break # indicator folder

                # Mark this parquet file as processed for this indicator folder
                processed_files_for_indicator = state.get("processed_parquet_files", {}).get(indicator_folder_name, [])
                if scdi_file_name not in processed_files_for_indicator: # Should always be true if not skipped
                    processed_files_for_indicator.append(scdi_file_name)
                # Update the state with the modified list
                state.setdefault("processed_parquet_files", {})[indicator_folder_name] = processed_files_for_indicator
                self._save_processing_state(state) # Save state after processing each parquet file

                self._save_accumulated_recipes_snapshot(all_event_recipes_data_for_run, print_progress)
                all_event_recipes_data_for_run = {} # {event_type: [list_of_scdi_refs]}

            # Check if all parquet files for the current indicator_folder_name are now processed
            all_parquets_processed_for_current_indicator = True
            if not country_files: # No files to process in this indicator folder
                all_parquets_processed_for_current_indicator = True # Considered complete
            else:
                for cf_name in country_files:
                    if cf_name not in state.get("processed_parquet_files", {}).get(indicator_folder_name, []):
                        all_parquets_processed_for_current_indicator = False
                        break

            if all_parquets_processed_for_current_indicator:
                completed_indicators_list = state.get("completed_indicator_folders", [])
                if indicator_folder_name not in completed_indicators_list:
                    completed_indicators_list.append(indicator_folder_name)
                    state["completed_indicator_folders"] = completed_indicators_list # Ensure assignment if list was created
                    if print_progress:
                        print(f"\nIndicator folder '{indicator_folder_name}' fully processed. Marking as complete.")
                    self._save_processing_state(state) # Save state after completing an indicator folder

        # Check if all indicator folders initially found are now marked as complete
        all_indicators_fully_completed_this_session = True
        if not indicator_folders: # No indicator folders to process at all
             pass # Considered complete by default
        else:
            for ind_f_name in indicator_folders:
                if ind_f_name not in state.get("completed_indicator_folders", []):
                    all_indicators_fully_completed_this_session = False
                    break
        
        if all_indicators_fully_completed_this_session:
            state["last_run_completed_fully"] = True
            if print_progress:
                print("\nAll indicator folders processed. Full run will be reset on next trigger unless forced.")
        else:
            # This should already be False, but explicitly ensure it if run was partial
            state["last_run_completed_fully"] = False 
            if print_progress:
                print("\nNot all indicator folders were processed in this run. Progress saved.")
        self._save_processing_state(state) # Final state save
        print("All recipe saving/merging operations complete.")

    def run_pipeline(self, generate=True, print_progress=True):
        """
        Orchestrates the pipeline: loading events, generating recipes,
        and optionally analyzing a specific recipe.

        Args:
            generate (bool): If True, generate/update recipes.
            print_progress (bool): Passed to generate_recipes and analyze_recipe for verbose output.
        """
        print("Starting EventRecipeGenerator pipeline...")
        self.load_events() # Load events data

        if self.events_data is None or (isinstance(self.events_data, dict) and not self.events_data):
             print("Pipeline cannot continue: Events data failed to load or is empty.")
             return

        if generate:
            self.generate_recipes(print_progress=print_progress)
        else:
            print("Skipping recipe generation phase.")

        print("EventRecipeGenerator pipeline finished.")


generator = EventRecipeGenerator()
generator.run_pipeline(generate=True, print_progress=True)
