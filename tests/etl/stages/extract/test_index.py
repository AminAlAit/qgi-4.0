# """TODO"""

# from unittest.mock import patch
# import pytest
# import pandas as pd
# from src.etl.stages.extract.index import extract_runner, get_indicators_dfs
# from src.etl.stages.extract.wb_extractor import get_indicator


# def test_extract_runner_success(sample_etl_input, mock_indicator):
#     """TODO"""
#     with patch('stages.extract.index.get_indicators_dfs', return_value=[mock_indicator]):
#         result = extract_runner(sample_etl_input)
#         assert len(result) == 1
#         assert result[0].id == mock_indicator.id


# def test_extract_runner_exception(sample_etl_input):
#     """TODO"""
#     with patch('stages.extract.index.get_indicators_dfs', side_effect=Exception("Test error")):
#         with pytest.raises(Exception) as e:
#             extract_runner(sample_etl_input)
#         assert "Data extraction failed" in str(e.value)


# def test_get_indicators_dfs_success(sample_etl_input, mock_indicator):
#     """TODO"""
#     with patch('stages.extract.wb_extractor.get_indicator', return_value=mock_indicator):
#         result = get_indicators_dfs(sample_etl_input)
#         assert len(result) == len(sample_etl_input["indicator_ids"])
#         assert result[0].id == mock_indicator.id


# def test_get_indicators_dfs_empty_input():
#     """TODO"""
#     etl_input = {"indicator_ids": []}
#     result = get_indicators_dfs(etl_input)
#     assert result == []


# def test_get_indicator_success():
#     """TODO"""
#     with patch('wbgapi.series.metadata.get') as mock_meta, \
#          patch('wbgapi.data.DataFrame') as mock_data:

#         mock_meta.return_value.metadata = {
#             "IndicatorName": "Test Indicator",
#             "License_Type": "CC-BY-4.0",
#             "Longdefinition": "Test definition",
#             "Othernotes": "Test notes",
#             "Periodicity": "Annual",
#             "Relatedsourcelinks": "example.com",
#             "Source": "World Bank",
#             "Topic": "Economics"
#         }
#         mock_data.return_value = pd.DataFrame({"country": ["USA"], "year": [2020], "value": [100]})

#         indicator = get_indicator("TEST_ID")
#         assert indicator.id == "TEST_ID"
#         assert indicator.name == "Test Indicator"
#         assert not indicator.data.empty


# def test_get_indicator_missing_metadata():
#     """TODO"""
#     with patch('wbgapi.series.metadata.get') as mock_meta, \
#          patch('wbgapi.data.DataFrame') as mock_data:

#         mock_meta.return_value.metadata = {}
#         mock_data.return_value = pd.DataFrame()

#         indicator = get_indicator("TEST_ID")
#         assert indicator.name == ""
#         assert indicator.license_type == ""
#         assert indicator.data.empty
