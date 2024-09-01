import utils.utilities as utils_
import ingestion
import utils.databases as db
import pandas as pd
import os

NAMESPACE = 'open_data_ingestion'
environment = 'test'
os.environ["environment"] = environment



config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "ingestion", NAMESPACE,'config', 'test_config.yaml') 
#config_path = 'src/ingestion/open_data_ingestion/config/config.yaml'
config = utils_.read_config(config_path)
## ingesting data into db
for dataset, args in config[NAMESPACE].items():
    staging_path = ingestion.ingestion.open_data_ingestion.open_data_ingestion.open_api_handler(config, dataset)
    data = pd.read_csv(staging_path)
    db.load_data(data,NAMESPACE, dataset.replace('-','_'), args['db_load_type'])
    query = '''SELECT name FROM sqlite_master WHERE type='table' '''
    print(db.get_query_df(query))

##performing unit test
#Getting the location data
sensor_locations = '''select * from open_data_ingestion_pedestrian_counting_system_sensor_locations'''
sensor_locations_df = db.get_query_df(sensor_locations)
assert len(sensor_locations_df) == 108
print(f"pass sensor_locations")

# Getting the count data
counts = '''select * from open_data_ingestion_pedestrian_counting_system_monthly_counts_per_hour'''
counts_df = db.get_query_df(counts)
assert len(counts_df) == 3523
print ("pass counts_df")


