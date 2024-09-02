import src.utils.utilities as utils 
import src.utils.databases as database_utils



def run_transform(NAMESPACE,attributes):
    ouptut_table_name = attributes['table_name']#'top_10_locations'
    sql = attributes['sql']
    
    staging_table_name = f'{ouptut_table_name}_stg'

    print('Connecting to ingestion db')
    source_connection = database_utils.get_db_connection(database_utils.SILVER_LAYER_DB_NAME)
    print('Connecting to modelled db')
    destination_connection = database_utils.get_db_connection(database_utils.GOLD_LAYER_DB_NAME)
    print('Running query')
    df = database_utils.get_query_df(sql,source_connection)
    print(f'writing to staging {staging_table_name} modelled db')
    database_utils.load_data(destination_connection, df,NAMESPACE, staging_table_name, 'replace')
    print(f'writing to {ouptut_table_name} modelled db')
    database_utils.replace_database(destination_connection, df, staging_table_name, ouptut_table_name, primary_key = None)
    print(f"writing csv reference output as {NAMESPACE}_{ouptut_table_name}_ref.csv")
    df.to_csv(f"{NAMESPACE}_{ouptut_table_name}_ref.csv", index= False)
    

