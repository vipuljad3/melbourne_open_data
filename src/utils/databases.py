import sqlite3
import pandas as pd
import os
import re

BRONZE_LAYER_NAME = 'sourcing'
SILVER_LAYER_DB_NAME = 'ingestion'
GOLD_LAYER_DB_NAME = 'modelled'

def get_db_connection(db_type):
# Connect to the in-memory SQLite database
    environment =  os.getenv("environment")
    try:
        connection = sqlite3.connect(f'{db_type}_{environment}.db')
        
        print(f"database connection successful {db_type}_{environment}.db")
        return connection
    except:
        print("failed to connect to the db")

def get_query_df(query,connection):
    #connection = get_db_connection()
    df = pd.read_sql(query, connection)
    return df



def upsert_database(conn, df, staging_table, master_table, primary_key):
    add_pk_to_sqlite_table(staging_table, primary_key, conn)
    # add_pk_to_sqlite_table(master_table, primary_key, conn)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({master_table});")
    #print(master_table_exists.fetchmany())
    if not  cursor.fetchall():
        
        print("Creating master table")
        column = pd.io.sql.get_schema(df, master_table).replace(f'CREATE TABLE \"{master_table}\"','')
        sql = f'''
        create table if not exists {master_table}{column};
        '''
        cursor.execute(sql)
        print('Adding primary key to master table')
        add_pk_to_sqlite_table(master_table, primary_key, conn)


    cursor.execute(f"PRAGMA table_info({master_table});")
    columns = [info[1] for info in cursor.fetchall()]
    upsert_sql  = f'''
                INSERT INTO {master_table}  
                SELECT *
                FROM {staging_table}
                WHERE true
                ON CONFLICT ({primary_key}) 
                DO UPDATE SET 
                {', '.join([f'{col} = excluded.{col}' for col in columns])};'''.replace ('index','\"index\"')
    print(upsert_sql)
    cursor.execute(upsert_sql)    
    conn.commit()            

def insert_database(conn, df, staging_table, master_table, primary_key = None):
    cursor = conn.cursor()
    column = pd.io.sql.get_schema(df, master_table).replace(f'CREATE TABLE \"{master_table}\"','')
    create_sql = sql = f'''
        create table if not exists {master_table}{column};
        '''
    cursor.execute(create_sql)
    insert_sql = f'''
                INSERT INTO {master_table}
                select * from {staging_table};
                '''
    cursor.execute(insert_sql)
    conn.commit()

def replace_database(conn, df, staging_table, master_table, primary_key = None):
    cursor = conn.cursor()
    column = pd.io.sql.get_schema(df, master_table).replace(f'CREATE TABLE \"{master_table}\"','')
    create_sql = sql = f'''
        create table if not exists {master_table}{column};
        '''
    cursor.execute(create_sql)
    truncate_sql = f'Delete from  {master_table}'
    cursor.execute(truncate_sql)
    insert_sql = f'''
                INSERT INTO {master_table}
                select * from {staging_table};
                '''
    cursor.execute(insert_sql)
    conn.commit()

# def create_transform_table(conn,table_name,sql):
#     cursor = conn.cursor()
#     sql = f'''create table if not exists {table_name} as  {sql}'''
#     print (sql)
#     cursor.execute(sql)
#     conn.commit()


def load_data(connection, df,namespace, table_name, load_condition):
    #connection = get_db_connection()
    #table_name = f'{namespace}_{table_name}'
    print(table_name)
    df.to_sql(table_name, connection, if_exists=load_condition, index=False)


def get_create_table_string(tablename, connection):
    sql = """
    select * from sqlite_master where name = "{}" and type = "table"
    """.format(tablename) 
    result = connection.execute(sql)

    create_table_string = result.fetchmany()[0][4]
    return create_table_string

def add_pk_to_create_table_string(create_table_string, colname):
    regex = "(\n.+{}[^,]+)(,)".format(colname)
    return re.sub(regex, "\\1 PRIMARY KEY,",  create_table_string, count=1)

def add_pk_to_sqlite_table(tablename, index_column, connection):
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

    create_and_drop_sql = template.format(tablename = tablename, cts = cts)
    connection.executescript(create_and_drop_sql)
    

