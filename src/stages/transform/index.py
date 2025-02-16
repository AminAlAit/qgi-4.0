
import pandas as pd


def transform_runner(dfs):
    transformed_dfs = []
    for df in dfs:
        pivoted_df = pivot_dataframe(df)
        result = rename_columns(pivoted_df)
        result, unmatched_countries = clean_country_names(result)
        result = delete_rows_by_country(result, unmatched_countries)

        # final step
        transformed_dfs.append(result)

    return transformed_dfs


def pivot_dataframe(df):
    """
    Reads a wide-format CSV file and pivots it into a long format.

    Parameters:
        file_path (str): Path to the input CSV file.

    Returns:
        pd.DataFrame: A DataFrame in long format with columns:
                      - 'economy': The economy code (e.g., ABW, AFE, AFG).
                      - 'year': The year extracted from column names (e.g., 1960, 1961).
                      - 'value': The corresponding value for the economy and year.
    """
    # Melt the DataFrame to convert year columns into rows
    df["economy"] = df.index
    df_long = pd.melt(
        df,
        id_vars=['economy'],  # Keep 'economy' as an identifier
        var_name='year',      # New column for years
        value_name='value'    # New column for values
    )

    # Extract the numeric part of the year (remove 'YR' prefix)
    df_long['year'] = df_long['year'].str.extract(r'(\d+)').astype(int)

    # Sort the DataFrame by economy and year for better readability
    df_long = df_long.sort_values(by=['economy', 'year']).reset_index(drop=True)

    return df_long


def rename_columns(df, column_mapping = None):
    if column_mapping is None:
        column_mapping = {
            'economy': 'country',
        }
    return df.rename(columns=column_mapping)


## TODO: figure out a better way to link "countries.csv" with this function,
## rather than reading it multiple times and taking out the "Unnamed" columns.
def clean_country_names(input_df):
    """
    Cleans the 'country' column of the input DataFrame by replacing codes with their corresponding country names.
    
    Parameters:
        input_df (pd.DataFrame): The input DataFrame with a 'country' column to be cleaned.
    
    Returns:
        tuple: A tuple containing:
            - pd.DataFrame: A DataFrame with the 'country' column replaced by country names.
            - list: A list of unmatched values from the 'country' column.
    """
    # Step 1: Load the reference CSV file
    reference_df = pd.read_csv(r"/workspaces/qgi-4.0/data/countries.csv")
    reference_df = reference_df.loc[:, ~reference_df.columns.str.contains("Unnamed")]

    # Ensure 'name_1' exists in the reference DataFrame
    if 'name_1' not in reference_df.columns:
        raise KeyError("The column 'name_1' is missing in the reference data.")

    # Step 2: Create a mapping dictionary
    mapping = {}
    for col in reference_df.columns:
        if col != 'name_1':  # Skip the 'name_1' column itself
            temp_mapping = reference_df.dropna(subset=[col]).set_index(col)['name_1'].to_dict()
            mapping.update(temp_mapping)

    # Step 3: Replace values in the 'country' column using the mapping
    unmatched_values = []  # List to store unmatched values
    input_df['country'] = input_df['country'].apply(
        lambda x: mapping.get(x, x)  # Replace if found in mapping, otherwise keep original
    )
    # Identify unmatched values
    unmatched_values = [
        value for value in input_df['country'] if value not in mapping.values() and value not in mapping.keys()
    ]

    return input_df, list(set(unmatched_values))


def delete_rows_by_country(input_df, countries_to_delete):
    """
    Deletes rows from the input DataFrame where the 'country' column matches any value in the given list.

    Parameters:
        input_df (pd.DataFrame): The input DataFrame.
        countries_to_delete (list): A list of country names or codes to delete.

    Returns:
        pd.DataFrame: A DataFrame with the specified rows removed.
    """
    # Filter out rows where the 'country' column matches any value in the list
    filtered_df = input_df[~input_df['country'].isin(countries_to_delete)]
    
    return filtered_df
