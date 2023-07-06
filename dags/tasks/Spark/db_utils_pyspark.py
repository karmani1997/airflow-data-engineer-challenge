import configparser
import os
from pyspark.sql import SparkSession

def get_spark_session():
    """
    This function is used to create a SparkSession
    :return: SparkSession object
    """
    spark = SparkSession.builder.getOrCreate()
    return spark

def get_db_url():
    """
    This function is used to get the PostgreSQL database URL
    :return: PostgreSQL database URL
    """
    config = configparser.ConfigParser()
    path = '/'.join((os.path.abspath(__file__).replace('\\', '/')).split('/')[:-1])
    config.read(os.path.join(path, 'etl_datapipeline.cfg'))
    return config['DB_CONFIG']['postgres_url']

def insert_into_table(table_name, df, df_schema):
    """
    This function is used to insert a DataFrame into a PostgreSQL table
    :param table_name: table name to which data is inserted
    :param df: DataFrame to be loaded into the database
    :param df_schema: schema of the DataFrame
    :return: None
    """
    spark = get_spark_session()
    df.write.format("jdbc").options(
        url=get_db_url(),
        driver="org.postgresql.Driver",
        dbtable=table_name,
        user=config['DB_CONFIG']['user'],
        password=config['DB_CONFIG']['password']
    ).mode("append").save()


def get_sql_data_into_df(query, current_month):
    """
    :param query: SQL query to load data into a DataFrame
    :param current_month: current month filter condition
    :return: DataFrame result from the query execution
    """
    spark = get_spark_session()
    return spark.sql(query.replace('$current_month$', current_month))


def get_aggregated_sql_data_into_df(query, previous_date, current_date):
    """
    :param query: SQL query to load data into a DataFrame
    :param previous_date: previous date filter
    :param current_date: current date filter
    :return: DataFrame result from the query execution
    """
    spark = get_spark_session()
    return spark.sql(query, previous_date=previous_date, current_date=current_date)


def delete_data_from_table(sql_query):
    """
    :param sql_query: delete query to delete records from a table
    :return: None
    """
    spark = get_spark_session()
    spark.sql(sql_query)


def get_df_from_db(sql_query):
    """
    :param sql_query: Execute query to fetch data from DB into DataFrame
    :return: DataFrame result from the query execution
    """
    spark = get_spark_session()
    return spark.sql(sql_query)
