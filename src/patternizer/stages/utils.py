"""
Module containing utility functions for logging, unique ID generation, and other
helper functions used across the patternization pipeline.
"""

import warnings
import random
import string
import time
from datetime import datetime
import numpy as np
import pandas as pd
from src.classes.index import Index

## Constants
STAGE = "patternize"
YEAR_START_LIMIT = 2000  # Assuming this is defined elsewhere

## Log legend
#  D:  Disqualified: the algorithm is trying to find, for example, a 5 year pattern in a 3 years long index
#  SK: Skipping: skipping pattern searching because index's length does not span beyond 3 years
#  S1: Saved: pattern already saved in db, skipping loop turn
#  S2: Saving: pattern is new to db, it is being saved to it
#  C1: Country patternization is done, country name saved in db table
#  C2: Country already patternized for this index, for this org, for this version
#  I1: Saved: Index patternized and already saved in db, no need to do it again, so skipping loop turn
#  I2: Index patternization is done, Index name saved in db table

def generate_unique_id(*args) -> str:
    """
    Generate a unique ID based on the provided parameters.
    The function accepts a variable number of arguments, which 
    should match the primary key structure of the target table.
    A random string is appended to ensure uniqueness.
    """
    random_chars = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    unique_id_parts = [str(arg) for arg in args] + [random_chars]
    return "_".join(unique_id_parts)

def get_org_version_main(index: Index) -> list:
    """
    Extracts essential information from an Index object.
    
    Parameters:
    - index (Index): An instance of the Index class.
    
    Returns:
    - list: A list containing organization, version, and main name.
    """
    return [index.org, index.version, index.main_polished_name]

def get_pattern_lengths_list(df: pd.DataFrame) -> list:
    """
    Generate a list of possible pattern lengths based on the dataset's year range.
    
    Parameters:
    - df (pd.DataFrame): The input DataFrame containing 'Year' column.
    
    Returns:
    - list: A list of integers representing possible pattern lengths.
    """
    min_year = df["Year"].min()
    max_year = df["Year"].max()
    return list(range(3, max_year - min_year + 1))

def import_and_fix_csv_column_names(csv_path: str, index: Index) -> pd.DataFrame:
    """
    Rename columns of a DataFrame based on the Index object's patternizable indexes.
    
    Parameters:
    - csv_path (str): Path to the CSV file.
    - index (Index): An instance of the Index class.
    
    Returns:
    - pd.DataFrame: DataFrame with corrected column names.
    """
    new_column_names = index.get_patternizable_indexes()
    df = pd.read_csv(csv_path)
    current_columns = [col for col in df.columns if col not in ["Year", "Country"]]
    if len(current_columns) != len(new_column_names):
        raise ValueError("Column count mismatch during renaming.")
    rename_dict = {old: new for old, new in zip(current_columns, new_column_names)}
    df.rename(columns=rename_dict, inplace=True)
    return df

def get_pattern_fromIndex_matrix_profile(country_ts, pattern_length):
    """
    Compute the matrix profile for a given time series and pattern length.
    
    Parameters:
    - country_ts: Time series data for a country.
    - pattern_length: Length of the pattern to identify.
    
    Returns:
    - tuple: Pattern, starting index, and matrix profile string.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        warnings.simplefilter("ignore", category=UserWarning)
        country_ts = [float(item) for item in country_ts]
        matrix_profile = stump(country_ts, m=pattern_length)
        matrix_profile_str = ','.join(map(str, matrix_profile))
    from_index = np.argmin(matrix_profile[:, 0])
    pattern = country_ts[from_index:from_index + pattern_length]
    return pattern, from_index, matrix_profile_str

def check_index_pattern_length_disqualification(
    org: str, version: int, main_name: str, index_name: str, pattern_lengths_list: list
) -> bool:
    """
    Check if an index should be disqualified from patternization based on pattern length.
    
    Parameters:
    - org (str): Organization name.
    - version (int): Version number.
    - main_name (str): Main index name.
    - index_name (str): Index name being checked.
    - pattern_lengths_list (list): List of pattern lengths.
    
    Returns:
    - bool: True if disqualified, False otherwise.
    """
    min_pattern_length = min(pattern_lengths_list)
    if min_pattern_length < 3:
        disqualified_reason = f"Pattern length {min_pattern_length} is less than 3 years."
        print(f"Index {index_name} is disqualified from patternizing: {disqualified_reason}")
        # Assuming generate_unique_id is defined elsewhere
        unique_id = generate_unique_id(org, version, main_name, index_name, disqualified_reason)
        # Database insertion logic here
        return True
    return False
