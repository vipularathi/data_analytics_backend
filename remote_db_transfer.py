import json
from datetime import datetime
from time import time, sleep
from sqlalchemy import create_engine, inspect
import sqlalchemy as sql
import sqlalchemy.exc as sql_exec
import pandas as pd
from sqlalchemy import insert, select
import psycopg2
from psycopg2 import sql
from common import today, yesterday

def copy_table(db_copy_from, db_copy_to, table_name):
    """
        This function copies a table from a remote database to a local database.

        Parameters:
        db_copy_from (str): The name of the remote database to copy from.
        db_copy_to (str): The name of the local database to copy to.
        table_name (str): The name of the table to copy.

        Returns:
        None
    """

    # Remote server (source)
    remote_host = "172.16.47.54"
    remote_dbname = f"{db_copy_from}"
    remote_user = "postgres"
    remote_password = "Vivek001"

    # Local server (destination)
    local_host = "localhost"
    local_dbname = f"{db_copy_to}"
    local_user = "postgres"
    local_password = "root"

    try:
        # Connect to the remote database
        remote_engine = create_engine(f'postgresql+psycopg2://{remote_user}:{remote_password}@{remote_host}:5432/{remote_dbname}')

        # Connect to the local database
        local_engine = create_engine(f'postgresql+psycopg2://{local_user}:{local_password}@{local_host}:5432/{local_dbname}')
        table_name = f'{table_name}'

        # Inspect the remote database to get table schema
        inspector = inspect(remote_engine)
        table_columns = inspector.get_columns(table_name)
        column_names = [column['name'] for column in table_columns]

        # Create a DataFrame with the column names and specify dtype as object
        data = pd.DataFrame(columns=column_names, dtype=object)

        # Create the table in the local database if it doesn't exist
        data.to_sql(table_name, local_engine, index=False, if_exists='replace')

        desired_date = str(input('If there is a data specific, Enter the desired date, else press enter : '))

        # Query to select data from the remote table based on the desired date, if date not selected then query the whole db
        if not desired_date:
            query = f'select * from {table_name}'
        else:
            desired_date = pd.to_datetime(desired_date).strftime('%Y-%m-%d')
            print(f'desired date is {desired_date}')
            query = f"select * from {table_name} where date(timestamp) = '{desired_date}' order by id"

        # Read data from the remote table into a pandas DataFrame
        df = pd.read_sql(query, remote_engine)

        # Write the DataFrame to the local database
        df.to_sql(table_name, local_engine, index=False, if_exists='append')

        print(f'Table {table_name} copied from remote to local')

    except psycopg2.Error as e:
        print(f'\n error is {e}')

if __name__ == "__main__":
    print(f'today is {today} and yesterday was {yesterday} and date alone is {today.strftime("%Y-%m-%d")}')
    db_copy_from = str(input('Enter the name of db to copy from : '))
    db_copy_to = str(input('Enter the name of db to copy to : '))
    table_name = str(input('Enter the name of the table to copy : '))
    copy_table(db_copy_from, db_copy_to, table_name)
