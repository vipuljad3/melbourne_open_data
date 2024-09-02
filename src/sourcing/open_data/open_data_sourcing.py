import requests
import pandas as pd
from datetime import datetime
from dateutil.tz import tzutc
import src.utils.utilities as utils

NAMESPACE = 'open_data'

def open_api_handler(config, database):
    """
    Handles the API call to open data API and processes the response into a DataFrame.

    Args:
        config (dict): Configuration dictionary containing API details, lookback settings, and data processing rules.
        database (str): The name of the dataset or database to be fetched and processed.

    Returns:
        str: The file path where the staged data is saved.

    Notes:
        This function checks if a lookback period is required. If lookback is not required, it fetches data 
        without date filters; otherwise, it collects data over a specified lookback period using the lookback_collect function.
    """
    url = config['open_api_url']
    if config[NAMESPACE][database]['lookback'] == False:
        date = None
        df = open_api_to_df(url, database, date)
    else:
        lookback_days = config[NAMESPACE][database]['lookback_days']
        source_date_column = config[NAMESPACE][database]['source_date_column']
        df = lookback_collect(url, database, lookback_days, source_date_column)
    
    field_list = list(df['fields'])
    df = pd.DataFrame(field_list)
    staging_path = utils.stage_data(NAMESPACE, database, df, 'csv', config[NAMESPACE][database]['overwrite_sourced'])
    return staging_path


def lookback_collect(url, database, lookback_days, source_date_column):
    """
    Collects data over a specified lookback period by making API requests for each date.

    Args:
        url (str): The base URL for the open data API.
        database (str): The name of the dataset to fetch.
        lookback_days (int): Number of days to look back from today.
        source_date_column (str): The column name in the dataset used to filter by date.

    Returns:
        DataFrame: A pandas DataFrame containing the concatenated data fetched from the API over the lookback period.

    Notes:
        This function constructs a list of dates for the lookback period and makes API requests for each date to fetch the relevant data.
    """
    today = datetime.now()
    subset_date = pd.to_datetime(today) - pd.DateOffset(days=lookback_days)
    dates = pd.date_range(start=subset_date, end=today)
    date_list = [date.strftime('%Y/%m/%d') for date in dates]
    print("Looking for these dates: \n", date_list)
    df = pd.DataFrame()

    for date in date_list:
        batch = open_api_to_df(url, database, date, source_date_column)
        print(len(batch))
        df = pd.concat([df, batch], ignore_index=True)
    
    return df


def open_api_to_df(url, database, date, source_date_column=None):
    """
    Fetches data from an open data API and converts it into a pandas DataFrame.

    Args:
        url (str): The base URL for the open data API.
        database (str): The name of the dataset to fetch.
        date (str): The specific date to filter data on (formatted as 'YYYY/MM/DD'). If None, fetches all available data.
        source_date_column (str, optional): The column name in the dataset used to filter by date. Required if a date is provided.

    Returns:
        DataFrame: A pandas DataFrame containing the data fetched from the API.

    Notes:
        Constructs API parameters based on whether a date filter is provided, and handles pagination by setting a large row count.
    """
    if date is not None:
        params = {
            "dataset": database,  
            "rows": 10000,
            "q": f"{source_date_column} = {date}",
            "sort": [source_date_column], 
            "format": "json", 
            "timezone": "UTC"  
        }
    else:
        params = {
            "dataset": database,  
            "rows": 10000,
            "start": 0, 
            "format": "json", 
            "timezone": "UTC"  
        }
    
    print(url, params)
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    records = data['records']
    df = pd.DataFrame(records)
    
    return df


def subset_date(df, lookback_date_column, lookback_days):
    """
    Subsets a DataFrame based on a date column and a specified lookback period.

    Args:
        df (DataFrame): The pandas DataFrame containing the data to subset.
        lookback_date_column (str): The name of the date column in the DataFrame.
        lookback_days (int): The number of days to look back from today.

    Returns:
        DataFrame: A pandas DataFrame containing data filtered to include only rows within the lookback period.
                   Returns None if no data falls within the lookback period.

    Notes:
        Converts the specified date column to datetime format and filters the DataFrame to include only records newer than the calculated lookback date.
    """
    df[lookback_date_column] = pd.to_datetime(df[lookback_date_column])
    now = datetime.now(tzutc())
    subset_date = pd.to_datetime(now - pd.DateOffset(days=lookback_days))
    
    if min(df[lookback_date_column] <= subset_date):
        df_lookback_days = df[df[lookback_date_column] >= subset_date]
        return df_lookback_days
    else:
        return None


