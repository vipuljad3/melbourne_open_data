import src.utils.utilities as utils 
import src.utils.databases as database_utils
import pandas as pd
import os
import shutil

def create_connection():
    """
    Establishes a connection to the silver layer database.

    Returns:
        sqlite3.Connection: A connection object to interact with the database.

    Notes:
        This function uses a utility function from the `database_utils` module to get the connection.
    """
    con = database_utils.get_db_connection(database_utils.SILVER_LAYER_DB_NAME)
    return con


def read_source_data(path):
    """
    Reads all CSV files from a given directory and concatenates them into a single DataFrame.

    Args:
        path (str): The directory path where the source CSV files are located.

    Returns:
        DataFrame: A pandas DataFrame containing the concatenated data from all CSV files in the specified directory.

    Notes:
        The function adds a 'load_ts' column to each DataFrame extracted from a CSV file to indicate the load timestamp derived from the filename.
    """
    objects = utils.list_objects_in_directory(path)
    df_list = []
    for filename in objects:
        df = pd.read_csv(os.path.join(path, filename), index_col=None, header=0)
        df['load_ts'] = filename.replace('.csv', '')
        df_list.append(df)
    df = pd.concat(df_list, axis=0, ignore_index=True)
    return df


def archive_ingested_data(path):
    """
    Moves ingested data files from the sourcing directory to the archival directory.

    Args:
        path (str): The directory path where the source CSV files are currently located.

    Notes:
        The function replaces the sourcing directory with the archival directory in the file path and moves each file.
    """
    archive_path = path.replace(utils.DATA_SOURCING_DIRECTORY, utils.DATA_ARCHIVAL_DIRECTORY)
    utils.check_directory(archive_path)
    objects = utils.list_objects_in_directory(path)
    for file in objects:
        file_path = os.path.join(path, file)
        archive_file_path = os.path.join(archive_path, file)
        print(f'Archiving {file_path} to {archive_file_path}')
        shutil.move(file_path, archive_file_path)


def ingest(job_attributes, NAMESPACE, DATASET):
    """
    Ingests data from source files into the database, applying the specified load type (upsert or insert).

    Args:
        job_attributes (dict): Dictionary containing job-specific attributes such as table name, primary key, load type, and date column.
        NAMESPACE (str): The namespace for the dataset, typically representing a broader data categorization.
        DATASET (str): The dataset name, which determines where in the namespace the data is located.

    Notes:
        The function handles both 'upsert' and 'insert' operations based on the load type specified in `job_attributes`. 
        After data ingestion, it archives the ingested files.
    """
    table_name = job_attributes['table_name']
    primary_key = job_attributes['primary_key']
    load_type = job_attributes['load_type']
    date_column = job_attributes['date_column']
    path = os.path.join(utils.LANDING_DATA_DIRECTORY, NAMESPACE, DATASET, utils.DATA_SOURCING_DIRECTORY)
    
    staging_table_name = f'{table_name}_stg'
    print('Connecting to DB')
    connection = create_connection()
    
    print('Reading csv files')
    source_df = read_source_data(path)
    print(f'converting date column to timestamp {date_column}')
    source_df[date_column] = pd.to_datetime(source_df[date_column])

    if load_type == 'upsert':
        print('Performing upsert')
        print('sorting values')
        df_sorted = source_df.sort_values(by=[primary_key, 'load_ts'])
        print('dropping duplicates and keeping last ones')
        df_latest = df_sorted.drop_duplicates(subset=primary_key, keep='last').reset_index(drop=True)
        print(f'Inserting data into staging table {staging_table_name}')
        database_utils.load_data(connection, df_latest, NAMESPACE, staging_table_name, 'replace')
        print(f'upserting data into Master table {table_name}')
        database_utils.upsert_database(connection, df_latest, staging_table_name, table_name, primary_key)
    else:
        print('Performing append')
        print(f'Inserting data into staging table {staging_table_name}')
        database_utils.load_data(connection, source_df, NAMESPACE, staging_table_name, 'replace')
        print(f'Appending data into Master table {table_name}')
        database_utils.insert_database(connection, source_df, staging_table_name, table_name, primary_key=None)
        
    archive_ingested_data(path)
