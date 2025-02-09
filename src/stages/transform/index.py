
import pandas as pd


def transform_runner(dfs):
    transformed_dfs = []
    for df in dfs:
        pivoted_df = pivot_dataframe(df)
        transformed_dfs.append(pivoted_df)

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
