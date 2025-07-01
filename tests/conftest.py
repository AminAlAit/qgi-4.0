"""TODO"""

import pandas as pd
import pytest
from src.pipelines.a_indicator_extractors.wb_indicators_etl.wb_indicators_etl import PipelineConfig


@pytest.fixture(name="dummy_countries_csv_path")
def mock_countries_csv(tmp_path):
    """Creates a dummy countries.csv file in a temporary directory."""
    # MODIFIED: Removed 'WLD,World' to correctly test the filtering logic
    csv_content = """code,name_1
AFE,Africa Eastern and Southern
USA,United States"""
    csv_path = tmp_path / "countries.csv"
    csv_path.write_text(csv_content)
    return str(csv_path)


@pytest.fixture(name="test_config")
def test_pipeline_config(tmp_path, dummy_countries_csv_path):
    """Creates a PipelineConfig object for testing, using the renamed fixture."""
    output_dir = tmp_path / "output"
    return PipelineConfig(
        output_path=str(output_dir),
        countries_csv_path=dummy_countries_csv_path,
        output_format="parquet",
        indicators=["SP.POP.TOTL"],
        file_size_limit=1000
    )


@pytest.fixture(name="sample_raw_dataframe")
def mock_raw_wb_dataframe():
    """Returns a sample raw DataFrame from the World Bank API."""
    data = {'YR1960': [100, 200, 300], 'YR1961': [110, 210, 310]}
    index = pd.Index(['USA', 'AFE', 'WLD'], name='economy')
    return pd.DataFrame(data, index=index)


@pytest.fixture(name="sample_indicator_metadata")
def mock_indicator_metadata():
    """Mocks the metadata object returned by wbgapi."""
    class MockMetadata:
        concept = "Test Concept"
        metadata = {"IndicatorName": "Population, total"}

    return MockMetadata()
