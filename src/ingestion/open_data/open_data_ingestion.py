import src.utils.utilities as utils 
import src.utils.databases as database_utils
import pandas as pd
import os
import shutil

def create_connestion():
    con = database_utils.get_db_connection(database_utils.SILVER_LAYER_DB_NAME)
    return con

def read_source_data(path):
    objects = utils.list_objects_in_directory(path)
    df_list = []
    for filename in objects:
        df = pd.read_csv(os.path.join(path,filename), index_col=None, header=0)
        df['load_ts'] = filename.replace('.csv','')
        df_list.append(df)
    df = pd.concat(df_list, axis=0, ignore_index=True)
    return df

def archive_ingested_data(path):
    archive_path = path.replace(utils.DATA_SOURCING_DIRECTORY, utils.DATA_ARCHIVAL_DIRECTORY)
    utils.check_directory(archive_path)
    objects = utils.list_objects_in_directory(path)
    for file in objects:
        file_path = os.path.join(path,file)
        archive_file_path = os.path.join(archive_path,file)
        print(f'archiving {file_path} to {archive_file_path}')
        shutil.move(file_path, archive_file_path)

def ingest(job_attributes, NAMESPACE, DATASET):
    table_name = job_attributes['table_name']
    primary_key = job_attributes['primary_key']
    load_type = job_attributes['load_type']
    date_column = job_attributes['date_column']
    path = os.path.join(utils.LANDING_DATA_DIRECTORY,NAMESPACE,DATASET,utils.DATA_SOURCING_DIRECTORY)
    
    staging_table_name  = f'{table_name}_stg'
    conncetion = create_connestion()
    
    source_df = read_source_data(path)
    source_df[date_column] = pd.to_datetime(source_df[date_column])
    if load_type == 'upsert':
        df_sorted = source_df.sort_values(by=[primary_key, 'load_ts'])
        df_latest = df_sorted.drop_duplicates(subset=primary_key, keep='last').reset_index(drop=True)
        print('inserting data into staging table')
        database_utils.load_data(conncetion, df_latest,NAMESPACE, staging_table_name, 'replace')
        database_utils.upsert_database(conncetion, df_latest, staging_table_name, table_name, primary_key)
    else:
        database_utils.load_data(conncetion, source_df,NAMESPACE, staging_table_name, 'replace')
        database_utils.insert_database(conncetion, source_df, staging_table_name, table_name, primary_key = None)
        
    archive_ingested_data(path)
