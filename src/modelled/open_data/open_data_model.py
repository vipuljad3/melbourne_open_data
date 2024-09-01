import src.utils.utilities as utils 
import src.utils.databases as database_utils
import pandas as pd



def run_transform(NAMESPACE,attributes):
    ouptut_table_name = attributes['table_name']#'top_10_locations'
    sql = attributes['sql']
    
    staging_table_name = f'{ouptut_table_name}_stg'
    source_connection = database_utils.get_db_connection(database_utils.SILVER_LAYER_DB_NAME)
    destination_connection = database_utils.get_db_connection(database_utils.GOLD_LAYER_DB_NAME)
    df = database_utils.get_query_df(sql,source_connection)
    database_utils.load_data(destination_connection, df,NAMESPACE, staging_table_name, 'replace')
    database_utils.replace_database(destination_connection, df, staging_table_name, ouptut_table_name, primary_key = None)
    

