import yaml
import os
import datetime

DATA_SOURCING_DIRECTORY = 'sourcing'
DATA_ARCHIVAL_DIRECTORY = 'archive'
LANDING_DATA_DIRECTORY = 'landing_zone'    ## BRONZE DATA
PRODUCTION_ENV_NAME = 'PROD'
TEST_ENV_NAME = 'TEST'
TEST_DATA_DIRECTORY = os.path.join('tests','sample_data')



def read_config(file):
    """
    Reads a YAML configuration file and returns its contents.

    Args:
        file (str): The path to the YAML configuration file.

    Returns:
        dict: The contents of the YAML file as a dictionary.

    Raises:
        yaml.YAMLError: If an error occurs while parsing the YAML file.
    """
    with open(file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def list_objects_in_directory(directory_path):
    """
    Lists all objects in a specified directory and prints them.

    Args:
        directory_path (str): The path to the directory.

    Returns:
        list: A list of names of the items in the directory.
    """
    items = os.listdir(directory_path)
    print(items)
    return items


def check_directory(directory_path):
    """
    Checks if a directory exists and creates it if it does not.

    Args:
        directory_path (str): The path to the directory to check or create.

    Prints:
        str: A message indicating whether the directory was created or already exists.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def remove_files_in_directory(directory_path):
    """
    Removes all files in a specified directory.

    Args:
        directory_path (str): The path to the directory whose files should be removed.

    Prints:
        str: A message indicating each file that was removed successfully.
    """
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"File '{filename}' removed successfully.")


def stage_data(namespace, path, df, type, remove_flag):
    """
    Stages data by saving a DataFrame to a specified path, in a specified format.

    Args:
        namespace (str): The namespace for the data staging.
        path (str): The path where the data should be staged.
        df (DataFrame): The pandas DataFrame to be staged.
        type (str): The file type for saving the DataFrame ('csv' or 'parquet').
        remove_flag (bool): Whether to remove existing files in the directory before staging.

    Returns:
        str: The full path to the staged data file.
    """
    if os.environ['environment'] == TEST_ENV_NAME:
        file_path = os.path.join(TEST_DATA_DIRECTORY, namespace, path)
    else: 
        file_path = os.path.join(LANDING_DATA_DIRECTORY, namespace, path, DATA_SOURCING_DIRECTORY)

    check_directory(file_path)

    if remove_flag:
        remove_files_in_directory(file_path)

    filename = f'{datetime.datetime.now()}.{type.lower()}'.replace('-', '').replace(' ', '_').replace(':', '')
    file_end_path = os.path.join(file_path, filename)
    print(df)

    if type.lower() == 'csv':
        df.to_csv(file_end_path, index=False)
    elif type.lower() == 'parquet':
        df.to_parquet(file_end_path, index=False)
    else:
        exit()

    return file_end_path
