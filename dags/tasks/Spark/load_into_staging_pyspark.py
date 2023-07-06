import os
import glob
import re
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, FloatType, TimestampType
from .db_utils_pyspark import insert_into_table, get_df_from_db

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def perform_staging_etl(**kwargs):
    """
     This is the driver function to load data into the staging_table.
     Step 1: Load the data frame from file location
     Step 2: Clean and preprocess the data frame
     Step 3: Insert the data frame into staging_table
     Step 4: Push the datetime_created field in the dataframe into context.
             This value will be used in the where clause of other tasks
    :param kwargs: push the datetime_created field into context to be used by other tasks
    :return: None
    """
    logging.info("Starting process to load data into staging")
    
    spark = SparkSession.builder.getOrCreate()
    
    staging_df = load_dataframe(spark)
    logging.info("Data frame loaded")
    
    preprocessed_df, prev_date = preprocess_dataframe(staging_df)
    logging.info("Data frame cleaned and preprocessed")

    insert_into_table('invoice_table', preprocessed_df, get_staging_schema())
    logging.info("Data loaded into table")
    
    kwargs['ti'].xcom_push(key='current_date', value=preprocessed_df.select('datetime_created').orderBy(col('datetime_created').desc()).first()['datetime_created'])
    kwargs['ti'].xcom_push(key='previous_date', value=prev_date)
    logging.info("ETL for staging table completed")


def load_dataframe():
    """
    Read the csv file from location and return the PySpark DataFrame
    :return: PySpark DataFrame which will be loaded into DB
    """

    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Define the directory path where the CSV files are located
    directory_path = os.path.expanduser('~/data_files_airflow/')

    print(directory_path)

    # Define the regex pattern for matching the desired file names
    file_pattern = r'invoices_\d{4}\d{2}\d{2}\.csv$'

    # Find all file paths that match the regex pattern
    matching_files = [file for file in glob.glob(directory_path + '*') if re.match(directory_path + file_pattern, file)]

    # Load the matching CSV files into a PySpark DataFrame
    dfs = []
    for file_path in matching_files:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        dfs.append(df)

    # Union the individual DataFrames if needed
    combined_df = dfs[0] if len(dfs) == 1 else dfs[0].unionAll(dfs[1:])

    return combined_df


def preprocess_dataframe(df):
    """
    :param df: Spark DataFrame which will be processed and cleaned
    :return: processed Spark DataFrame which is to be loaded in DB
    """
    df = df.withColumnRenamed('total', 'amount')
    
    # Convert the required types
    df = df.withColumn('amount', df['amount'].cast(FloatType()))
    df = df.withColumn('venue_id', df['venue_id'].cast(StringType()))
    df = df.withColumn('invoice_id', df['invoice_id'].cast(StringType()))
    df = df.withColumn('datetime_created', df['datetime_created'].cast(TimestampType()))
    
    # Filling missing values of amount with zero
    df = df.na.fill({'amount': 0})
    
    # Sort the DataFrame by the 'datetime_created' column in ascending order
    df = df.orderBy('datetime_created')
    
    # Get the previous date
    prev_date = df.select('datetime_created').first()['datetime_created']

    return df, prev_date


def get_staging_schema():
    """
    :return: Schema which will be used to insert data into the DB
    """
    return {
        "venue_id": StringType(),
        "invoice_id": StringType(),
        "datetime_created": TimestampType(),
        "amount": FloatType()
    }
