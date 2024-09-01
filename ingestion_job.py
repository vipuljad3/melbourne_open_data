import src.ingestion.open_data.open_data_ingestion as open_data_ingestion
import src.utils.utilities as utils_

import src.utils.databases as db_utils_
import os

os.environ["environment"] = utils_.PRODUCTION_ENV_NAME


JOB_TYPE = db_utils_.SILVER_LAYER_DB_NAME
NAMESPACE = 'open_data'

config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "src",JOB_TYPE, NAMESPACE,'config', 'config.yaml') 

config = utils_.read_config(config_path)

for DATASET in config[NAMESPACE]:
    print(utils_.LANDING_DATA_DIRECTORY)
    print(DATASET)
    job_attributes = config[NAMESPACE][DATASET]
    open_data_ingestion.ingest(job_attributes, NAMESPACE, DATASET)