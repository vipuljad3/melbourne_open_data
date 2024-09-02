import sqlite3
import pandas as pd
import os
import re

BRONZE_LAYER_NAME = 'sourcing'
SILVER_LAYER_DB_NAME = 'ingestion'
GOLD_LAYER_DB_NAME = 'modelled'

def get_db_connection(db_type):
    """
    Establishes a connection to an in-memory SQLite database based on the specified database type and environment.

    Args:
        db_type (str): The type of the database to connect to.

    Returns:
        sqlite3.Connection: A connection object to the SQLite database if successful; otherwise, None.

    Prints:
        str: A message indicating whether the database connection was successful or failed.
    """
    environment = os.getenv("environment")
    try:
        connection = sqlite3.connect(f'{db_type}_{environment}.db')
        print(f"Database connection successful: {db_type}_{environment}.db")
        return connection
    except Exception as e:
        print(f"Failed to connect to the database: {e}")


def get_query_df(query, connection):
    """
    Executes a SQL query on a database connection and returns the result as a pandas DataFrame.

    Args:
        query (str): The SQL query to execute.
        connection (sqlite3.Connection): The SQLite database connection.

    Returns:
        DataFrame: The result of the SQL query as a pandas DataFrame.
    """
    df = pd.read_sql(query, connection)
    return df


def upsert_database(conn, df, staging_table, master_table, primary_key):
    """
    Performs an upsert (insert or update) operation from a staging table to a master table in an SQLite database.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.
        df (DataFrame): The pandas DataFrame containing the data to be upserted.
        staging_table (str): The name of the staging table.
        master_table (str): The name of the master table.
        primary_key (str): The primary key column name for conflict resolution.

    Prints:
        str: SQL statement being executed and status messages for table creation and primary key addition.

    Notes:
        This function adds a primary key to the staging table, creates the master table if it doesn't exist, and then performs the upsert operation.
    """
    add_pk_to_sqlite_table(staging_table, primary_key, conn)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({master_table});")
    
    if not cursor.fetchall():
        print("Creating master table")
        column = pd.io.sql.get_schema(df, master_table).replace(f'CREATE TABLE \"{master_table}\"', '')
        sql = f'create table if not exists {master_table}{column};'
        cursor.execute(sql)
        print('Adding primary key to master table')
        add_pk_to_sqlite_table(master_table, primary_key, conn)

    cursor.execute(f"PRAGMA table_info({master_table});")
    columns = [info[1] for info in cursor.fetchall()]
    upsert_sql = f'''
        INSERT INTO {master_table}  
        SELECT *
        FROM {staging_table}
        WHERE true
        ON CONFLICT ({primary_key}) 
        DO UPDATE SET 
        {', '.join([f'{col} = excluded.{col}' for col in columns])};
    '''.replace('index', '\"index\"')
    
    print(upsert_sql)
    cursor.execute(upsert_sql)
    conn.commit()


def insert_database(conn, df, staging_table, master_table, primary_key=None):
    """
    Inserts data from a staging table into a master table in an SQLite database.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.
        df (DataFrame): The pandas DataFrame containing the data to be inserted.
        staging_table (str): The name of the staging table.
        master_table (str): The name of the master table.
        primary_key (str, optional): The primary key column name. Defaults to None.

    Notes:
        This function creates the master table if it doesn't exist and inserts all data from the staging table.
    """
    cursor = conn.cursor()
    column = pd.io.sql.get_schema(df, master_table).replace(f'CREATE TABLE \"{master_table}\"', '')
    create_sql = f'create table if not exists {master_table}{column};'
    cursor.execute(create_sql)
    
    insert_sql = f'''
        INSERT INTO {master_table}
        SELECT * FROM {staging_table};
    '''
    cursor.execute(insert_sql)
    conn.commit()


def replace_database(conn, df, staging_table, master_table, primary_key=None):
    """
    Replaces all data in a master table with data from a staging table in an SQLite database.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.
        df (DataFrame): The pandas DataFrame containing the data to replace.
        staging_table (str): The name of the staging table.
        master_table (str): The name of the master table.
        primary_key (str, optional): The primary key column name. Defaults to None.

    Notes:
        This function deletes all data in the master table and replaces it with data from the staging table.
    """
    cursor = conn.cursor()
    column = pd.io.sql.get_schema(df, master_table).replace(f'CREATE TABLE \"{master_table}\"', '')
    create_sql = f'create table if not exists {master_table}{column};'
    cursor.execute(create_sql)
    
    truncate_sql = f'DELETE FROM {master_table};'
    cursor.execute(truncate_sql)
    
    insert_sql = f'''
        INSERT INTO {master_table}
        SELECT * FROM {staging_table};
    '''
    cursor.execute(insert_sql)
    conn.commit()


def load_data(connection, df, namespace, table_name, load_condition):
    """
    Loads data from a pandas DataFrame into a specified table in an SQLite database.

    Args:
        connection (sqlite3.Connection): The SQLite database connection.
        df (DataFrame): The pandas DataFrame to load into the database.
        namespace (str): A namespace prefix for the table name.
        table_name (str): The name of the table to load data into.
        load_condition (str): The loading condition ('replace', 'append', etc.).

    Notes:
        This function loads data into the database based on the specified condition.
    """
    df.to_sql(table_name, connection, if_exists=load_condition, index=False)


def get_create_table_string(tablename, connection):
    """
    Retrieves the SQL CREATE TABLE statement for a specified table in an SQLite database.

    Args:
        tablename (str): The name of the table.
        connection (sqlite3.Connection): The SQLite database connection.

    Returns:
        str: The CREATE TABLE SQL statement for the specified table.
    """
    sql = f"""
    SELECT * FROM sqlite_master WHERE name = "{tablename}" AND type = "table";
    """
    result = connection.execute(sql)
    create_table_string = result.fetchmany()[0][4]
    return create_table_string


def add_pk_to_create_table_string(create_table_string, colname):
    """
    Modifies a CREATE TABLE SQL string to add a primary key constraint to a specified column.

    Args:
        create_table_string (str): The original CREATE TABLE SQL statement.
        colname (str): The column name to be set as the primary key.

    Returns:
        str: The modified CREATE TABLE SQL statement with the primary key constraint.
    """
    regex = f"(\n.+{colname}[^,]+)(,)"
    return re.sub(regex, "\\1 PRIMARY KEY,", create_table_string, count=1)


def add_pk_to_sqlite_table(tablename, index_column, connection):
    """
    Adds a primary key constraint to a specified column in an SQLite table.

    Args:
        tablename (str): The name of the table to modify.
        index_column (str): The column to set as the primary key.
        connection (sqlite3.Connection): The SQLite database connection.

    Notes:
        This function modifies the table schema to add a primary key to an existing table by renaming the table,
        creating a new table with the primary key, copying the data over, and dropping the old table.
    """
    cts = get_create_table_string(tablename, connection)
    cts = add_pk_to_create_table_string(cts, index_column)
    template = """
    BEGIN TRANSACTION;
        ALTER TABLE {tablename} RENAME TO {tablename}_old_;
        {cts};
        INSERT INTO {tablename} SELECT * FROM {tablename}_old_;
        DROP TABLE {tablename}_old_;
    COMMIT TRANSACTION;
    """
    create_and_drop_sql = template.format(tablename=tablename, cts=cts)
    connection.executescript(create_and_drop_sql)
