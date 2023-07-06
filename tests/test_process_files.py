import os
import unittest
from unittest.mock import patch
from dags.tasks.process_files import check_file_exists, wait_for_file, move_files


class TestFileOperations(unittest.TestCase):

    def setUp(self):
        self.source_folder = os.path.expanduser('~/data_files_airflow/')
        self.destination_folder = os.path.expanduser('~/processed_files_airflow/')

    def test_check_file_exists(self):
        # Create a dummy file in the source folder
        dummy_file = 'invoices_20191015.csv'
        dummy_file_path = os.path.join(self.source_folder, dummy_file)
        open(dummy_file_path, 'w').close()

        # Test if the check_file_exists function returns True
        self.assertTrue(check_file_exists())

        # Remove the dummy file
        os.remove(dummy_file_path)

        # Test if the check_file_exists function returns False
        self.assertFalse(check_file_exists())

    @patch('time.sleep', return_value=None)
    @patch('dags.tasks.process_files.check_file_exists', side_effect=[False, True])
    def test_wait_for_file(self, mock_check_file_exists, mock_sleep):
        # Call the wait_for_file function
        result = wait_for_file()

        # Check if the function returns 'file_exists'
        self.assertEqual(result, 'file_exists')

        # Verify that the check_file_exists function was called twice
        self.assertEqual(mock_check_file_exists.call_count, 2)

        # Verify that the time.sleep function was called once
        mock_sleep.assert_called_once_with(10)

    def test_move_files(self):
        # Create a dummy file in the source folder
        dummy_file = 'invoices_20191015.csv'
        dummy_file_path = os.path.join(self.source_folder, dummy_file)
        open(dummy_file_path, 'w').close()

        # Call the move_files function
        move_files()

        # Verify that the file has been moved to the destination folder
        destination_file_path = os.path.join(self.destination_folder, dummy_file)
        self.assertTrue(os.path.exists(destination_file_path))

        # Clean up the dummy file
        os.remove(destination_file_path)

    
if __name__ == '__main__':
    unittest.main()

