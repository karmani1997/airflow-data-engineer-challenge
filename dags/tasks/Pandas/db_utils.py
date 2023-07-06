"""
This file contains common DB util methods used in the tasks.
"""

import configparser
import os
import pandas as pd

from sqlalchemy import create_engine


def get_db_engine():
    """
    This function is used to create DB engine to connect to DB
    :return: DB engine to execute queries
    """
    config = configparser.ConfigParser()
    path = '/'.join((os.path.abspath(__file__).replace('\\', '/')).split('/')[:-1])
    config.read(os.path.join(path, 'etl_datapipeline.cfg'))
    return create_engine('{}://{}:{}@{}:{}/{}'.format(*config['DB_CONFIG'].values()))


def insert_into_table(table_name, df, df_dtypes):
    """
    This function is used to insert a data frame which the corresponding dtypes into table
    :param table_name: table name to which data is inserted
    :param df: dataframe to be loaded into DB
    :param df_dtypes: data type of the columns
    :return: None
    """
    engine = get_db_engine()
    connection = engine.connect()
    df.to_sql(table_name, connection, if_exists='append', index=False, dtype=df_dtypes)
    connection.close()


def get_sql_data_into_df(query, current_date):
    """

    :param query: get data loaded into dataframe with query
    :param current_date: current date filter condition
    :return: pandas dataframe result from the query execution
    """
    engine = get_db_engine()
    return pd.read_sql_query(query, engine, params={"current_date": current_date})


def get_aggregated_sql_data_into_df(query, previous_date, current_date):
    """

    :param query: Get data with only changed ranks from previous months data in aggregated table
    :param current_date: current date filter
    :param previous_date: previous date filter
    :return:  pandas dataframe result from the query execution
    """
    engine = get_db_engine()
    return pd.read_sql_query(query, engine,
                             params={"current_date": current_date,
                                     "previous_date": previous_date})


def delete_data_from_table(sql_query):
    """

    :param sql_query: delete query to delete records from table
    :return: None
    """
    engine = get_db_engine()
    engine.execute(sql_query)


def get_df_from_db(sql_query):
    """

    :param sql_query: Execute query to fetch data from DB into dataframe
    :return: pandas data frame
    """
    engine = get_db_engine()
    return pd.read_sql_query(sql_query, engine)
