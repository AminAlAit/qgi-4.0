## TODO: Replace unittest with pytest

# import unittest
# from unittest.mock import MagicMock, patch
# from stages.extract.index import extract_runner, get_indicators_dfs
# from stages.transform.index import transform_runner, pivot_dataframe, rename_columns, clean_country_names, delete_rows_by_country
# from stages.load.index import load_runner
# import pandas as pd

# class TestStages(unittest.TestCase):
#     def test_extract_runner(self):
#         # Mock get_indicators_dfs
#         with patch('stages.extract.index.get_indicators_dfs') as mock_get:
#             mock_get.return_value = [MagicMock()]

#             # Test extract runner
#             etl_input = {"indicator_ids": ["SP.POP.TOTL"]}
#             result = extract_runner(etl_input)

#             # Assertions
#             mock_get.assert_called_once_with(etl_input)
#             self.assertIsNotNone(result)

#     def test_transform_runner(self):
#         # Mock indicators
#         mock_indicators = [MagicMock()]

#         # Test transform runner
#         result = transform_runner(mock_indicators)

#         # Assertions
#         self.assertIsNotNone(result)

#     def test_load_runner(self):
#         # Mock indicators
#         mock_indicators = [MagicMock()]

#         # Test load runner
#         load_runner(mock_indicators)

#         # Assertions
#         # Add specific assertions based on your implementation

#     def test_pivot_dataframe(self):
#         # Create sample DataFrame
#         df = pd.DataFrame({
#             'economy': ['USA', 'USA'],
#             'YR2020': [1, 2],
#             'YR2021': [3, 4]
#         })

#         # Test pivot functionality
#         transformed = pivot_dataframe(df)
#         self.assertIn("year", transformed.columns)
#         self.assertIn("value", transformed.columns)
#         self.assertEqual(len(transformed), 4)

#     def test_rename_columns(self):
#         # Create sample DataFrame
#         df = pd.DataFrame({'economy': ['USA', 'CAN']})

#         # Test column renaming
#         renamed = rename_columns(df)
#         self.assertIn("country", renamed.columns)
#         self.assertNotIn("economy", renamed.columns)

#     def test_clean_country_names(self):
#         # Create sample DataFrame
#         df = pd.DataFrame({'country': ['USA', 'GBR']})

#         # Test country name cleaning
#         cleaned_df, _ = clean_country_names(df)
#         self.assertIn("United States", cleaned_df["country"].values)
#         self.assertIn("United Kingdom", cleaned_df["country"].values)

#     def test_delete_rows_by_country(self):
#         # Create sample DataFrame
#         df = pd.DataFrame({'country': ['USA', 'XXX']})

#         # Test row deletion
#         filtered_df = delete_rows_by_country(df, ['XXX'])
#         self.assertNotIn("XXX", filtered_df["country"].values)

# if __name__ == "__main__":
#     unittest.main()
