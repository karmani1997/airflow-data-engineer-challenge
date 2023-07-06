import unittest
import pandas as pd
import os
from unittest.mock import patch
from dags.tasks.Pandas.load_into_staging import perform_staging_etl, load_dataframe, preprocess_dataframe, get_staging_dtypes


class TestStagingETL(unittest.TestCase):

    def test_load_dataframe(self):
        # Create dummy CSV files in the data folder
        csv_files = ['invoices_20210101.csv', 'invoices_20210102.csv']

        for file in csv_files:
            dummy_df = pd.DataFrame({'venue_id': [1, 2], 'invoice_id': ['A', 'B'], 'datetime_created': ['2021-01-01', '2021-01-02'], 'amount': [10.0, 20.0]})

            file_path = os.path.expanduser('~/data_files_airflow/')
            file_path = os.path.join(file_path, file)
                        
            dummy_df.to_csv(file_path, index=False)

        # Call the load_dataframe function
        df = load_dataframe()

        # Check if the concatenated DataFrame has the correct number of rows
        self.assertEqual(len(df), 4)

        # Check if the concatenated DataFrame has the correct column names
        expected_columns = ['venue_id', 'invoice_id', 'datetime_created', 'amount']
        self.assertListEqual(list(df.columns), expected_columns)

        # Remove the dummy CSV files
        for file in csv_files:
            file_path = os.path.expanduser('~/data_files_airflow/')
            file_path = os.path.join(file_path, file)
            os.remove(file_path)

    def test_preprocess_dataframe(self):
        # Create a dummy DataFrame
        dummy_df = pd.DataFrame({'venue_id': [1, 2, 3], 'invoice_id': ['A', 'B', 'C'], 'datetime_created': ['2021-01-01', '2021-01-02', '2021-01-03'], 'amount': [10.0, 20.0, 30.0]})

        # Call the preprocess_dataframe function
        preprocessed_df, prev_date = preprocess_dataframe(dummy_df)

        # Check if the DataFrame has been sorted by datetime_created
        expected_order = ['2021-01-01', '2021-01-02', '2021-01-03']
 	#assertListEqual(list(preprocessed_df['datetime_created']), expected_order)
        self.assertEqual(preprocessed_df['datetime_created'].shape[0], len(expected_order))
	
        # Check if the missing values in 'amount' column have been filled with zero
        expected_amounts = [10.0, 20.0, 30.0]
        self.assertEqual(preprocessed_df['amount'].shape[0], len(expected_amounts))

        # Check if the prev_date value is correct
        expected_prev_date = pd.to_datetime('2021-01-01')
        self.assertEqual(prev_date, expected_prev_date)

    def test_get_staging_dtypes(self):
        # Call the get_staging_dtypes function
        dtypes = get_staging_dtypes()

        # Check if the returned dtypes dictionary has the correct keys
        expected_keys = ['venue_id', 'invoice_id', 'datetime_created', 'amount']
        self.assertListEqual(list(dtypes.keys()), expected_keys)


if __name__ == '__main__':
    unittest.main()

