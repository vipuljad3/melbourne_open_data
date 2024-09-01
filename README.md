# pedestrian-counting-system-melbourne-od

The Pedestrian Counting System (counts per hour) has gone through some updates in the rescent years and the ddl of the incoming datasets have been changed along with the API. The API now only retains data from 24th April 2023, The older data has been archived and is located here :
https://melbournetestbed.opendatasoft.com/api/datasets/1.0/pedestrian-counting-system-monthly-counts-per-hour/attachments/pedestrian_counting_system_monthly_counts_per_hour_may_2009_to_14_dec_2022_csv_zip/

This pipeline will ingest data from the new API.
To load data from old API, Please run **extract_archived_zip_count.py**

### OPEN DATA INGESTION PIPELINE
the open data ingestion pipeline is located here:        
```
pedestrian-counting-system-melbourne-od/ingestion/open_data_ingestion
```
With the following structure

        ├── config.yaml
        ├── data
        │   ├── pedestrian-counting-system-monthly-counts-per-hour
        │   │   └── 2023-06-29 14:32:50.776210.csv
        │   └── pedestrian-counting-system-sensor-locations
        │       └── 2023-06-29 14:31:48.354138.csv
        ├── open_data_ingestion.py
The config.yaml file contains the configuration of datasets to be ingested as following:
`````
open_api_url : https://data.melbourne.vic.gov.au/api/records/1.0/search/

open_data_ingestion :
  pedestrian-counting-system-sensor-locations: 
    lookback : False
    overwrite_sourced : True
    db_load_type : replace

  pedestrian-counting-system-monthly-counts-per-hour :
    lookback : True
    lookback_days : 60
    overwrite_sourced : True
    db_load_type : replace

`````
    lookback_days : looks for the data for the last n number of days. 
    overwrite_sourced : [True/False] removes the staging data located in the data/ directory
    db_load_type : [append/replace] loads the data into sqllite db either with append or replace.

**open_data_ingestion.py** calls the API with set params and if there is a need for lookback days it will generate calls for each day and append each record, Finally the staging dataset will be saved in data/ directory.

### The Utils
The utils contains set of generic functions to be used while running the ingestion framework.
```
        ├── databases.py
        └── utilities.py
```
The databases.py contains generic sqllite database functions such as 
get_db_connection : connects to the local db. [can be configured to connect to any sqllite db host]
get_query_df : gets the query and runs the sql, returns the result dataframe.
load_data : takes in table-name and data and inserts records in the dataframe depending on the type of load [append/replace].

The utilities.py contains generic read and write functions such as :
read_config : reads config file and parses into a dictionary.
check_directory : checks if directory exists creates if does not exist.
remove_files_in_directory : removes files in given directory.
stage_data: stages the data in the local data folder in given namespace.

The run_ingestion_pipeline.py does the job to run the ingestion framework, load the data into staging table (GENERALLY HANDLED BY ANY ORCHESTRATOR LIKE AIRFLOW. In this instance we will run it manually).

#### Running the pipeline. 
in the terminal run:

```
git clone https://github.com/vipuljad3/pedestrian-counting-system-melbourne-od.git
cd pedestrian-counting-system-melbourne-od
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 run_ingestion_pipeline.py
python3 extract_archived_zip_count.py
```

#### Unit Testing.
unit testing is performed by setting the environment variable to test and reading test_config.yaml file.
The simple unit test includes assertion methods to check if the count of the rows match.


#### The data analysis.
There are two files which can be run for data analysis.
```
 ├── data_analysis.py (python file)
 └── data_analysis.ipynb (jupyter notebook)
 ```
They do the same job
For analysis documentation please refer to the comments in **data_analysis.py**
Each step is thoroughly defined.












