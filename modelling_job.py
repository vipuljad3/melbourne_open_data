import src.modelled.open_data.open_data_model as model
import src.utils.utilities as utils_
import src.utils.databases as db_utils_
import os 

os.environ["environment"] = utils_.PRODUCTION_ENV_NAME
JOB_TYPE = db_utils_.GOLD_LAYER_DB_NAME
NAMESPACE = 'open_data'

config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "src",JOB_TYPE, NAMESPACE,'config', 'config.yaml') 

config = utils_.read_config(config_path)

for DATASET in config[NAMESPACE]:
    job_attributes = config[NAMESPACE][DATASET]
    model.run_transform(NAMESPACE, job_attributes)