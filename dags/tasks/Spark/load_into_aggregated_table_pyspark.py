import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, FloatType, TimestampType
from .db_utils_pyspark import insert_into_table, get_aggregated_sql_data_into_df

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def populate_aggregated_data(**kwargs):
    """
    This function is the driver function.
    Step 1: pull the current_month and previous_month values from context
    Step 2: use the keys and query staging table.
    Step 3: Preprocess and clean the table
    Step 4: Insert the data into the invoice_aggregated_data_table table

    :param kwargs: context value holding the current_date and previous_date values
    :return: None
    """
    logging.info("Starting process to load data into aggregated table")
    
    ti = kwargs['ti']
    current_date = ti.xcom_pull(key='current_date', task_ids='insert_into_staging_table')
    previous_date = ti.xcom_pull(key='previous_date', task_ids='insert_into_staging_table')
    logging.info('current_date_field: {} '.format(current_date))
    logging.info('previous_date_field: {} '.format(previous_date))

    spark = SparkSession.builder.getOrCreate()
    
    staging_data = get_staging_data(spark, current_date, previous_date)
    logging.info('Data fetched from staging table')
    
    df = preprocess_dataframe(staging_data)
    logging.info('Data cleaned and preprocessed')
    
    insert_into_table('invoice_aggregated_data_table', df, get_aggregated_schema())
    logging.info('Data loaded into aggregated table')
    
    kwargs['ti'].xcom_push(key='current_date', value=current_date)
    logging.info("ETL for loading data into aggregated table completed")


def get_staging_data(spark, current_date, previous_date):
    """
    :param spark: SparkSession object
    :param previous_date: previous date data for querying
    :param current_date: current date data for querying
    :return: DataFrame from the SQL query executed
    """
    sql_query = f'''
    SELECT *
    FROM invoice_table
    WHERE datetime_created BETWEEN '{previous_date}' AND '{current_date}';
    '''

    return get_aggregated_sql_data_into_df(spark, sql_query)


def preprocess_dataframe(df):
    """
    :param df: Spark DataFrame
    :return: cleaned DataFrame
    """
    df = df.withColumn('datetime_created', col('datetime_created').cast(TimestampType()))
    
    # Group the DataFrame by 'venue_id' and 'datetime_created', and calculate the sum of 'amount' for each group
    aggregated_df = df.groupBy('venue_id', col('datetime_created').cast('date')).sum('amount')
    
    # Rename the columns
    aggregated_df = aggregated_df.withColumnRenamed('venue_id', 'venue_id')
    aggregated_df = aggregated_df.withColumnRenamed('CAST(datetime_created AS DATE)', 'datetime_created')
    aggregated_df = aggregated_df.withColumnRenamed('sum(amount)', 'amount')
    
    return aggregated_df


def get_aggregated_schema():
    """
    :return: Schema which will be used to insert data into the invoice_aggregated_data_table
    """
    return {
        "venue_id": StringType(),
        "datetime_created": TimestampType(),
        "amount": FloatType()
    }
