"""
This file contains all the operations needed to move the files
from source folder to destination folder.
Operations:
    - Use the keys and query staging table.
    - Pre process and clean the dataframe.
    - move the files into the destination folder
"""
import logging
import shutil
import os
import time
import glob
import re

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def check_file_exists():

    directory_path = os.path.expanduser('~/data_files_airflow/')
    # Define the regex pattern for matching the desired file names
    file_pattern = r'invoices_\d{4}\d{2}\d{2}\.csv$'

    # Find all file paths that match the regex pattern
    matching_files = [file for file in glob.glob(directory_path + '*') if re.match(directory_path + file_pattern, file)]
    if len(matching_files) > 0:
        return True
        
    return False

def wait_for_file(**kwargs):

 
    
    while not check_file_exists():
        time.sleep(10)  # Wait for 10 seconds before checking again
    
    return 'file_exists'

def move_files(**kwargs):

    source_folder = os.path.expanduser('~/data_files_airflow/')
    destination_folder = os.path.expanduser('~/processed_files_airflow/')

    
    # Get all files in the source folder
    files = os.listdir(source_folder)
    
    # Filter only CSV files
    csv_files = [f for f in files if f.endswith('.csv')]
    
    # Move CSV files from source folder to destination folder
    for file in csv_files:
        source_path = os.path.join(source_folder, file)
        destination_path = os.path.join(destination_folder, file)
        shutil.move(source_path, destination_path)
    logging.info("Files has been moved from the soource folder to the destination")
    
    
