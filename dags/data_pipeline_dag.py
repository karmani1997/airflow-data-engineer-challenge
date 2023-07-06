"""
This dag file contains all the pipeline tasks to ingest, transform and push data into a database

DAG : Data_pipeline_dag

Task 1: Check if the new csv file exists in the location.
Task 2: Extract, transform and Load the csv data into a "staging_table".
Task 3: Read from the staging table, aggregated and insert data
        into the "invoice_aggregated_data_table" table.
Task 4: Read the latest data inserted into "invoice_aggregated_data_table"
        table and load into "popular_destination_current_month" table.
Task 5: Once all the above tasks are successfully completed,
        delete the staging data from "staging_table" table.

Data Flow :csv -> staging_table-> popular_destination_aggregated->popular_destination_current_month

"""

from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.postgres_operator import PostgresOperator

#from tasks.load_into_popular_destinations import populate_popular_dest_data
from tasks.Pandas.load_into_staging import perform_staging_etl
from tasks.Pandas.load_into_aggregated_table import populate_aggregated_data

#from tasks.Spark.load_into_staging_pyspark import perform_staging_etl
#from tasks.Spark.load_into_aggregated_table_pyspark import populate_aggregated_data


from tasks.process_files import wait_for_file, move_files



# Default arguments with default parameters
default_args = {
    'owner': 'Mehtab',
    'start_date': datetime(2023, 7, 1),
    'retries': 10,
    'retry_delay': timedelta(seconds=60)
}

# creating DAG object and using it in all tasks within it
'*/10 * * * *'
with DAG('Data_pipeline_dag', default_args=default_args, schedule_interval='*/10 * * * *',
        catchup=True) as dag:
    
    # Task to check if the .csv file exists in the data folder    
    check_file_operator = PythonOperator(task_id='check_csv_file_existence',
                                            provide_context=True,
                                            python_callable=wait_for_file)

    # Task to load the csv data into a staging table
    prepare_staging_table = PythonOperator(task_id='insert_into_staging_table',
                                           provide_context=True,
                                           python_callable=perform_staging_etl)
    # Task to load data from staging into table containing aggregated data
    prepare_aggregated_table = PythonOperator(task_id='insert_into_aggregated_table',
                                           python_callable=populate_aggregated_data,
                                           provide_context=True)

    # Move the processed files into another folder
    move_data_files = PythonOperator(task_id='move_processed_files',
                                                python_callable=move_files,
                                                provide_context=True)


    # Executing the tasks in order
    check_file_operator >> prepare_staging_table >> prepare_aggregated_table >> move_data_files 