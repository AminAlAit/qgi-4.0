## TODO: Replace unittest with pytest

# import unittest
# from unittest.mock import MagicMock, patch
# from etl_runner.index import etl
# from stages.extract.index import extract_runner
# from stages.transform.index import transform_runner
# from stages.load.index import load_runner


# class TestETLRunner(unittest.TestCase):
#     def setUp(self):
#         self.mock_indicators = [MagicMock()]  # Mock indicators

#     @patch('stages.extract.index.extract_runner')
#     @patch('stages.transform.index.transform_runner')
#     @patch('stages.load.index.load_runner')
#     def test_etl(self, mock_load, mock_transform, mock_extract):
#         # Mock extract runner
#         mock_extract.return_value = self.mock_indicators

#         # Mock transform runner
#         mock_transform.return_value = self.mock_indicators

#         # Mock load runner
#         mock_load.return_value = None

#         # Test ETL function
#         etl_input = {
#             "indicator_ids": ["SP.POP.TOTL", "PV.EST"],
#             "config": {
#                 "output_format": "parquet",
#                 "output_path": "/workspaces/qgi-4.0/data/etl_output",
#                 "file_size_limit": 1000000
#             }
#         }
#         etl(etl_input)

#         # Assertions
#         mock_extract.assert_called_once_with(etl_input)
#         mock_transform.assert_called_once_with(self.mock_indicators)
#         mock_load.assert_called_once_with(self.mock_indicators)

# if __name__ == "__main__":
#     unittest.main()
