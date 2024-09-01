import src.sourcing.open_data.open_data_sourcing as open_data_sourcing
import src.utils.utilities as utils_

import src.utils.databases as db_utils_
import pandas as pd
import os

environment = utils_.PRODUCTION_ENV_NAME
JOB_TYPE = db_utils_.BRONZE_LAYER_NAME
NAMESPACE = 'open_data'
#DATASET = 'pedestrian-counting-system-monthly-counts-per-hour'
os.environ["environment"] = environment

config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "src",JOB_TYPE, NAMESPACE,'config', 'config.yaml') 


config = utils_.read_config(config_path)

for DATASET in config[NAMESPACE]:
    #$job_attributes = config[NAMESPACE][DATASET]
    open_data_sourcing.open_api_handler(config, DATASET)
