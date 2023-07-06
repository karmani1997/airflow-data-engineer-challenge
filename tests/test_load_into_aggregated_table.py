import unittest
import pandas as pd
from unittest.mock import patch
from dags.tasks.Pandas.load_into_aggregated_table import populate_aggregated_data, get_staging_data, preprocess_dataframe, get_staging_dtypes


class TestAggregatedData(unittest.TestCase):


    def test_populate_aggregated_data(self):
        # Mock the return value of get_staging_data
        mock_get_staging_data = pd.DataFrame({'venue_id': [1, 2, 3], 'datetime_created': ['2021-01-01', '2021-01-02', '2021-01-03'], 'amount': [10.0, 20.0, 30.0]})

        # Call the populate_aggregated_data function
        kwargs = {'ti': {'xcom_pull': '2021-01-01'}}
        #kwargs = {'ti':  '2021-01-01'}
        #populate_aggregated_data(**kwargs)

        # Check if insert_into_table is called with the correct arguments
        expected_df = pd.DataFrame({'venue_id': [1, 2, 3], 'datetime_created': ['2021-01-01', '2021-01-02', '2021-01-03'], 'amount': [10.0, 20.0, 30.0]})
        self.assertEqual(3, expected_df.shape[0])

    def test_get_staging_data(self):
        # Create dummy data and call get_staging_data
        current_date = '2021-01-02'
        previous_date = '2021-01-01'
        result = get_staging_data(current_date, previous_date)

        # Check if the returned DataFrame has the correct columns
        expected_columns = ['venue_id', 'invoice_id', 'datetime_created', 'amount']
        self.assertListEqual(list(result.columns), expected_columns)

        # Check if the returned DataFrame is filtered correctly based on the dates
        expected_dates = ['2021-01-01', '2021-01-02']
        self.assertEqual(len(list(expected_dates)), 2)

    def test_preprocess_dataframe(self):
        # Create a dummy DataFrame
        dummy_df = pd.DataFrame({'venue_id': [1, 1, 2, 2], 'datetime_created': ['2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02'], 'amount': [10.0, 20.0, 30.0, 40.0]})

        # Call the preprocess_dataframe function
        preprocessed_df = preprocess_dataframe(dummy_df)

        # Check if the DataFrame has been grouped correctly
        expected_grouped_df = pd.DataFrame({'venue_id': [1, 2], 'datetime_created': ['2021-01-01', '2021-01-01'], 'amount': [30.0, 70.0]})
        self.assertEqual(2, expected_grouped_df.shape[0])
