import yaml
import os
import datetime

DATA_SOURCING_DIRECTORY = 'sourcing'
DATA_ARCHIVAL_DIRECTORY = 'archive'
LANDING_DATA_DIRECTORY = 'landing_zone'    ## BRONZE DATA
PRODUCTION_ENV_NAME = 'PROD'
TEST_ENV_NAME = 'TEST'



## Reads the config file with yaml extension
def read_config(file):
    with open(file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

def list_objects_in_directory(directory_path):
    items = os.listdir(directory_path)
    print(items)
    return items

def check_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")

def remove_files_in_directory(directory_path):
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"File '{filename}' removed successfully.")

def stage_data(namespace, path, df, type, remove_flag):
    ## Pandas does not support windows directory 
    file_path = os.path.join(LANDING_DATA_DIRECTORY,namespace,path,DATA_SOURCING_DIRECTORY)
    check_directory(file_path)
    if remove_flag == True:
        remove_files_in_directory(file_path)
    filename =f'{datetime.datetime.now()}.{type.lower()}'.replace('-','').replace(' ','_').replace(':','')
    file_end_path = os.path.join(file_path, filename)
    print(df)
    if type.lower() == 'csv':
        df.to_csv(file_end_path, index = False)
    elif type.lower() == 'parquet':
        df.to_parquet(file_end_path, index = False)
    else:
        exit()
    
    return file_end_path