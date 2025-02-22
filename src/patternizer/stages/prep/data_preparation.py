"""
Data Preparation Module

Handles data ingestion, preprocessing, and initial database setup for the patternization pipeline.
"""

import pandas as pd
import time
import mysql.connector as mysql_connector
from src.classes.index import Index
from src.constant.constant import (
    TABLES_TO_DELETE,
    DB_CREDENTIALS,
    TEMPLATE_INDEX_TABLE_QUERY
)


def import_and_fix_csv_column_names(csv_path: str, index: Index) -> pd.DataFrame:
    """
    Imports CSV data and renames columns based on the provided Index object.
    
    Parameters:
        csv_path (str): Path to the CSV file.
        index (Index): Index object containing patternizable indexes.
    
    Returns:
        pd.DataFrame: Processed DataFrame with corrected column names.
    """
    new_column_names = index.get_patternizable_indexes()
    df = pd.read_csv(csv_path)
    current_columns = [col for col in df.columns if col not in ["Year", "Country"]]

    if len(current_columns) != len(new_column_names):
        raise ValueError("Column count mismatch during renaming.")

    rename_dict = {old: new for old, new in zip(current_columns, new_column_names)}
    df.rename(columns=rename_dict, inplace=True)
    return df


def get_pattern_lengths_list(df: pd.DataFrame) -> list:
    """
    Generates a list of possible pattern lengths based on the dataset's time range.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing the dataset.
    
    Returns:
        list: List of integers representing possible pattern lengths.
    """
    return list(range(3, df["Year"].max() - df["Year"].min() + 1))


def delete_specified_tables():
    """
    Deletes specified tables to reset the database state before processing.
    """
    for table in TABLES_TO_DELETE:
        delete_table(table)


def delete_table(table_name: str):
    """
    Drops a specific table from the database.
    
    Parameters:
        table_name (str): Name of the table to delete.
    """
    query = f"DROP TABLE IF EXISTS {table_name};"
    mysql_execute_query((query, ()))


def mysql_execute_query(
    query_params: tuple,
    max_retries: int = 99999,
    retry_delay: int = 1,
    credentials: dict = DB_CREDENTIALS
):
    """
    Executes a MySQL query with retry logic.
    
    Parameters:
        query_params (tuple): Tuple containing the SQL query and parameters.
        max_retries (int): Maximum number of retries.
        retry_delay (int): Delay between retries in seconds.
        credentials (dict): Database connection credentials.
    
    Returns:
        The query result (either affected rows or fetched data).
    """
    attempt = 0
    remarkable_attempts = [100, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 45000, 5000]

    while attempt < max_retries:
        try:
            conn = mysql_connector.connect(
                user=credentials["user"],
                password=credentials["password"],
                host=credentials["host"],
                database=credentials["database"]
            )
            break
        except Exception as err:
            attempt += 1
            if attempt in remarkable_attempts:
                print(f"!!! {err} !!!")
                print(f"Retrying in {retry_delay} seconds... (Attempt: {attempt})")
            if attempt >= max_retries:
                raise Exception("Max retries reached. Could not connect to MySQL.")
            time.sleep(retry_delay)

    cursor = conn.cursor()
    query, params = query_params

    if len(params) == 13:
        adjusted_params = params[:7] + (float(params[7]),) + params[8:]
        cursor.execute(query, adjusted_params)
    else:
        cursor.execute(query, params)

    if query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
        conn.commit()
        result = cursor.rowcount
    else:
        result = cursor.fetchall()

    cursor.close()
    conn.close()
    return result


def log_index_data(csv_file_path: str, index_obj: Index):
    """
    Processes indexes from a CSV file and updates the database schema accordingly.
    
    Parameters:
        csv_file_path (str): Path to the CSV file.
        index_obj (Index): Index object containing metadata.
    """
    df = pd.read_csv(csv_file_path)
    csv_columns = [col for col in df.columns if col not in ["Year", "Country"]]

    result_dict = {"inserted": [], "updated": [], "kept": []}

    # Insert organization and version if not exists
    insert_org_query = "INSERT INTO `organization` (org_name) VALUES (%s)"
    insert_version_query = "INSERT INTO `year` (year) VALUES (%s)"

    mysql_execute_query((insert_org_query, (index_obj.org,)))
    mysql_execute_query((insert_version_query, (index_obj.version,)))

    # Fetch existing indexes
    fetch_query = "SELECT id, index_name, version FROM `index` WHERE org = %s AND main_name = %s"
    existing_indexes = mysql_execute_query((fetch_query, (index_obj.org, index_obj.main_polished_name)))

    existing_index_info = {item[1]: {"id": item[0], "version": item[2]} for item in existing_indexes}

    for column in csv_columns:
        if column not in existing_index_info:
            # Insert new index
            insert_query = """
            INSERT INTO `index` (org, main_name, index_name, version)
            VALUES (%s, %s, %s, %s)
            """
            mysql_execute_query((insert_query, (index_obj.org, index_obj.main_polished_name, column, index_obj.version)))
            new_id = mysql_execute_query((
                "SELECT id FROM `index` WHERE org = %s AND main_name = %s AND index_name = %s AND version = %s",
                (index_obj.org, index_obj.main_polished_name, column, index_obj.version)
            ))[0][0]
            result_dict["inserted"].append(new_id)
        else:
            if existing_index_info[column]["version"] < index_obj.version:
                # Update existing index version
                update_query = "UPDATE `index` SET version = %s WHERE id = %s"
                mysql_execute_query((
                    update_query,
                    (index_obj.version, existing_index_info[column]["id"])
                ))
                result_dict["updated"].append(existing_index_info[column]["id"])
            else:
                result_dict["kept"].append(existing_index_info[column]["id"])

    # Process data for each index
    for status in ["inserted", "updated"]:
        for index_id in result_dict[status]:
            process_index(index_id, csv_file_path, index_obj)

    for kept_id in result_dict["kept"]:
        print(f"Index {kept_id} is up to date.")

    print("Data preparation completed.")


def process_index(index_id: int, csv_file_path: str, index_obj: Index):
    """
    Creates and populates a table for a specific index.
    
    Parameters:
        index_id (int): ID of the index in the database.
        csv_file_path (str): Path to the CSV file.
        index_obj (Index): Index object containing metadata.
    """
    df = pd.read_csv(csv_file_path)
    table_name = f"{index_id}_index"
    delete_table(table_name)

    create_table_query = TEMPLATE_INDEX_TABLE_QUERY.format(table_name_placeholder=table_name)
    mysql_execute_query((create_table_query, ()))

    index_name = mysql_execute_query((
        "SELECT index_name FROM `index` WHERE id = %s",
        (index_id,)
    ))[0][0]

    for _, row in df[["Year", "Country", index_name]].iterrows():
        country_id = get_id_by_value("country", row["Country"], "name_1")
        insert_query = f"INSERT INTO `{table_name}` (year_fk, country_fk, value) VALUES (%s, %s, %s)"
        value = row[index_name] if not pd.isna(row[index_name]) else None
        mysql_execute_query((insert_query, (row["Year"], country_id, value)))


def get_id_by_value(table: str, value: str, column: str = None):
    """
    Retrieves the ID of a record in a table based on its value.
    
    Parameters:
        table (str): Name of the database table.
        value (str): Value to search for.
        column (str): Specific column to search (optional).
    
    Returns:
        int: ID of the matching record or None if not found.
    """
    if column:
        query = f"SELECT id FROM `{table}` WHERE `{column}` = %s"
        result = mysql_execute_query((query, (value,)))
        return result[0][0] if result else None

    # Search all columns if no specific column provided
    columns_query = "SHOW COLUMNS FROM `%s`" % table
    columns = [col[0] for col in mysql_execute_query((columns_query, ()))]

    for col in columns:
        query = f"SELECT id FROM `{table}` WHERE `{col}` = %s"
        result = mysql_execute_query((query, (value,)))
        if result:
            return result[0][0]
    return None
