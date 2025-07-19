"""TODO"""

import os
import pandas as pd
import pyarrow.parquet as pq
from src.pipelines.a_indicator_extractors.wb_indicators_etl.wb_indicators_etl import (
    Extractor,
    Transformer,
    Loader,
    Pipeline,
    Indicator,
)


# Test the Extractor by mocking the wbgapi calls
def test_extractor_execute(mocker, sample_raw_dataframe, sample_indicator_metadata):
    """Verify that the extractor correctly calls the API and builds Indicator objects."""
    # Mock the external dependency
    mocker.patch("wbgapi.series.metadata.get", return_value=sample_indicator_metadata)
    mocker.patch("wbgapi.data.DataFrame", return_value=sample_raw_dataframe)

    extractor = Extractor()
    indicators = extractor.execute(["SP.POP.TOTL"])

    assert len(indicators) == 1
    indicator = indicators[0]
    assert indicator.id == "SP.POP.TOTL"
    assert indicator.indicator_name == "Population, total"
    assert not indicator.data.empty


# Test the Transformer's logic
def test_transformer_execute(dummy_countries_csv_path, sample_raw_dataframe):
    """Verify that the transformer correctly pivots, renames, and cleans the data."""
    transformer = Transformer(countries_csv_path=dummy_countries_csv_path)

    # Create a sample indicator to transform
    indicator = Indicator()
    indicator.id = "SP.POP.TOTL"
    indicator.data = sample_raw_dataframe

    transformed_indicators, unmatched_report = transformer.execute([indicator])

    # Check the transformed data
    transformed_df = transformed_indicators[0].data
    assert list(transformed_df.columns) == ["country", "year", "value"]
    assert "United States" in transformed_df["country"].values
    assert "Africa Eastern and Southern" in transformed_df["country"].values
    assert "World" not in transformed_df["country"].values  # Should be removed

    # Check the unmatched report
    assert "SP.POP.TOTL" in unmatched_report
    # FIXED: Check for the unmapped code 'WLD', not the name 'World'
    assert "WLD" in unmatched_report["SP.POP.TOTL"]


# Test the Loader's file writing
def test_loader_execute(tmp_path):
    """Verify that the loader correctly saves a parquet file."""
    loader = Loader()

    # Create a sample transformed indicator
    indicator = Indicator()
    indicator.id = "SP.POP.TOTL"
    indicator.data = pd.DataFrame({"country": ["USA"], "year": [2020], "value": [100]})

    loader.execute([indicator], output_path=str(tmp_path), output_format="parquet")

    # Check that the file was created
    expected_file = tmp_path / "SP.POP.TOTL" / "SP.POP.TOTL.parquet"
    assert expected_file.exists()

    # Check the content of the file
    df = pd.read_parquet(expected_file)
    assert len(df) == 1


def test_loader_skips_empty_data(tmp_path):
    """Verify that the loader skips indicators with no data."""
    loader = Loader()
    indicator = Indicator()
    indicator.id = "EMPTY.DATA"
    indicator.data = pd.DataFrame()  # Empty dataframe

    # FIXED: Use the 'tmp_path' fixture for the output path
    skipped = loader.execute([indicator], str(tmp_path), "parquet")

    assert "EMPTY.DATA" in skipped


# Test the full pipeline integration
def test_pipeline_run(
    mocker, test_config, sample_raw_dataframe, sample_indicator_metadata
):
    """Test the full pipeline run, mocking external systems (API and config loading)."""
    # Mock all external dependencies
    # FIXED: Corrected the patch path to not include 'src.'
    mocker.patch(
        "src.pipelines.a_indicator_extractors.wb_indicators_etl.wb_indicators_etl.Pipeline._load_config",
        return_value=test_config,
    )
    mocker.patch("wbgapi.series.metadata.get", return_value=sample_indicator_metadata)
    mocker.patch("wbgapi.data.DataFrame", return_value=sample_raw_dataframe)

    # Initialize and run the pipeline
    pipeline = Pipeline(
        config_path="dummy_config.yaml"
    )  # Path doesn't matter as _load_config is mocked
    result = pipeline.run()

    # Assert the final result and side effects
    assert result.status == "Success"
    assert result.processed_indicators == ["SP.POP.TOTL"]

    # Construct the expected path from the test_config fixture
    expected_file = os.path.join(
        test_config.output_path, "SP.POP.TOTL", "SP.POP.TOTL.parquet"
    )
    assert os.path.exists(expected_file)

    # Check content of the created parquet file
    df = pq.read_table(expected_file).to_pandas()
    assert "United States" in df["country"].values
