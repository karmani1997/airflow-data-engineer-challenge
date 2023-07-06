
"""
This file contains all the operations needed to pull data
from staging table into invoice_aggregated_data_table table.
Operations:
    - Use the keys and query staging table.
    - Pre process and clean the dataframe.
    - Insert data into the invoice_aggregated_data_table table
"""
import logging
import pandas as pd
import os
import datetime
from sqlalchemy import String, Float, DateTime
from .db_utils import insert_into_table, get_aggregated_sql_data_into_df

# Define the path to the log folder
log_folder_path = os.path.expanduser('~/logs_files_airflow/')
log_filename = datetime.datetime.now().strftime("log_load_into_staging%Y-%m-%d.log")
log_filename = os.path.join(log_folder_path, log_filename)



# Configure logging to save logs to the specified file
logging.basicConfig(filename=log_filename, filemode='a', level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def populate_aggregated_data(**kwargs):
    """
    This function is the driver function.
    Step 1: pull the current_date and previous_date values from context
    Step 2 : use the keys and query staging table.
    Step 3 : Pre process and clean the table
    Step 4 : Insert the data into the invoice_aggregated_data_table table

    :param kwargs: context value holding the current_date and previous_date values
    :return: None
    """
    logging.info("Starting process to load data into aggregated table")
    ti = kwargs['ti']
    current_date = ti.xcom_pull(key='current_date', task_ids='insert_into_staging_table')
    previous_date = ti.xcom_pull(key='previous_date', task_ids='insert_into_staging_table')
    logging.info('current_date_field: {} '.format(current_date))
    logging.info('previous_date_field: {} '.format(previous_date))

    sql_query_data = get_staging_data(current_date, previous_date)
    logging.info('Data fetched from staging table')
    df = preprocess_dataframe(sql_query_data)
    logging.info('Data cleaned and pre processed')
    insert_into_table('invoice_aggregated_data_table', df, get_staging_dtypes())
    logging.info('Data loaded into aggregated table')
    kwargs['ti'].xcom_push(key='current_date', value=current_date)
    logging.info("ETL for loading data into aggregated table completed")


def get_staging_data(current_date, previous_date):
    """

    :param previous_date: previous date data for querying
    :param current_date: current date data for querying
    :return: dataframe from the sql query executed
    """
    sql_query = '''
    SELECT *
    FROM invoice_table
    WHERE datetime_created BETWEEN %(previous_date)s AND %(current_date)s;
    '''

    return get_aggregated_sql_data_into_df(sql_query, previous_date, current_date)


def preprocess_dataframe(sql_query_df):
    """

    :param sql_query_df: pandas dataframe
    :return: cleaned dataframe
    """

    sql_query_df['datetime_created'] = pd.to_datetime(sql_query_df['datetime_created'])

    # Group the dataframe by 'venue_id' and 'datetime_created', and calculate the sum of 'total' for each group
    aggregated_df = sql_query_df.groupby(['venue_id', pd.Grouper(key='datetime_created', freq='D')]).sum()

    # Reset the index to get a clean dataframe
    aggregated_df = aggregated_df.reset_index()
    return aggregated_df


def get_staging_dtypes():
    """
    :return: dt type of the invoice_aggregated_data_table table
    """
    return {"venue_id": String(), "datetime_created": DateTime(), "amount": Float()}



def write_to_aggregated_table(df):
    """
    :param df: processed pandas dataframe which will be loaded into DB
    :return: None
    """
    insert_into_table('invoice_aggregated_data_table', df, get_staging_dtypes())
