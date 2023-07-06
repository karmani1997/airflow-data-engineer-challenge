"""
This file contains the task to read the csv file and load the data into staging_table.

Operations:
    - Read csv file from file location.
    - Pre process and clean the data in the data frame.
    - based on the location look_up table, data in pick_up and drop_off columns are updated
    - insert the processed data frame into DB.
    - Push xcom value which is the month field as it will be used by other task for queries.

"""

import logging
import pandas as pd
import glob
import re
import os
import datetime
from sqlalchemy import String, DateTime, Float
from statistics import mode
from .db_utils import insert_into_table, get_df_from_db


# Define the path to the log folder
log_folder_path = os.path.expanduser('~/logs_files_airflow/')
log_filename = datetime.datetime.now().strftime("log_load_into_staging%Y-%m-%d.log")
log_filename = os.path.join(log_folder_path, log_filename)



# Configure logging to save logs to the specified file
logging.basicConfig(filename=log_filename, filemode='a', level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def perform_staging_etl(**kwargs):
    """
     This is the driver function to load data into the staging_table.
     Step 1: Load the data frame from file location
     Step 2: Clean and pre process the data frame
     Step 3: Insert the data frame into staging_table
     Step 4: Push the datetime_created field in dataframe into context.
             This value will be used in where clause of other tasks
    :param kwargs: push the datetime_created field into context to be used by other tasks
    :return: None
    """
    logging.info("Starting process to load data into staging")
    staging_df = load_dataframe()
    logging.info("Data frame loaded")
    preprocessed_df, prev_date = preprocess_dataframe(staging_df)
    logging.info("Data frame cleaned and pre processed")

    # loc_df = get_location_data()
    # df = update_location_with_lookup(preprocessed_df, loc_df)

    insert_into_table('invoice_table', preprocessed_df, get_staging_dtypes())
    logging.info("Data loaded into table")
    kwargs['ti'].xcom_push(key='current_date', value=preprocessed_df['datetime_created'].tail(1).iloc[0])
    kwargs['ti'].xcom_push(key='previous_date', value=prev_date)
    logging.info("ETL for staging table completed")



def load_dataframe():
    """
    Read the csv file from location and return the pandas dataframe
    :return:  pandas data frame which will be loaded into DB
    """

    # Define the directory path where the CSV files are located
    directory_path = os.path.expanduser('~/data_files_airflow/')

    #print (directory_path)

    # Define the regex pattern for matching the desired file names
    file_pattern = r'invoices_\d{4}\d{2}\d{2}\.csv$'

    # Find all file paths that match the regex pattern
    matching_files = [file for file in glob.glob(directory_path + '*') if re.match(directory_path + file_pattern, file)]
 
    # Load the matching CSV files into pandas
    dfs = []
    for file_path in matching_files:
        df = pd.read_csv(file_path)
        dfs.append(df)

    # Concatenate the individual DataFrames if needed
    combined_df = pd.concat(dfs)

    return combined_df


def preprocess_dataframe(df):
    """
    :param df: pandas data frame which will be processed and cleaned
    :return: processed data frame which is to be loaded in DB
    """
    df.rename(columns={'total': 'amount'},inplace=True)
    
    #Convert the required type
    df.amount = df.amount.astype(float)
    df.venue_id = df.venue_id.astype(str)
    df.invoice_id = df.invoice_id.astype(str)
    df['datetime_created'] = pd.to_datetime(df['datetime_created'])
    
    # filling missing value of amount with zero value
    df['amount'].fillna(0, inplace=True)

    # Sort the DataFrame by the 'date' column in ascending order
    df = df.sort_values('datetime_created')


    # updating the date value with mode of the data frame value
    prev_date = df['datetime_created'].head(1).iloc[0]

    return df, prev_date


def get_staging_dtypes():
    """
    :return: dt type which will be used to insert data into the DB
    """
    return {"venue_id": String(), "invoice_id": String(), "datetime_created": DateTime(),
            "amount": Float()}
