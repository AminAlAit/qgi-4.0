"""TODO"""

import os
import pandas as pd
from pipelines.b_scdi_finder.scdi_finder import ScdiFinder


def test_discover_indicators(finder_test_config):
    """Verify that the pipeline correctly finds all parquet files."""
    finder = ScdiFinder(config=finder_test_config)
    discovered = finder._discover_indicators()
    
    assert len(discovered) == 2
    indicator_ids = {id for id, path in discovered}
    assert "INDICATOR_A" in indicator_ids
    assert "INDICATOR_B" in indicator_ids


def test_preprocess_data():
    """Verify that preprocessing correctly interpolates and fills NaN values."""
    # This test doesn't need the full class __init__
    finder = ScdiFinder.__new__(ScdiFinder)
    
    df = pd.DataFrame({
        'country': ['USA', 'USA', 'USA', 'CAN', 'CAN'],
        'year': [2000, 2001, 2002, 2000, 2001],
        'value': [10.0, pd.NA, 20.0, 5.0, pd.NA]
    })

    preprocessed = finder._preprocess_data(df, countries=['USA', 'CAN'])

    # FIXED: Assert that both countries were processed, as each had only one NaN.
    assert len(preprocessed) == 2

    # Check USA: NaN was interpolated as (10 + 20) / 2 = 15.0
    usa_data = preprocessed[0]
    assert usa_data['country'] == 'USA'
    assert usa_data['values'][1] == 15.0

    # ADDED: Check Canada: NaN at the end was back-filled with the previous value (5.0)
    can_data = preprocessed[1]
    assert can_data['country'] == 'CAN'
    assert can_data['values'][1] == 5.0


def test_sdci_finding_logic(finder_test_config):
    """Verify that the core correlation logic finds correct pairs and skips others."""

    finder = ScdiFinder(config=finder_test_config)
    preprocessed_data = [
        {'country': 'USA', 'data': pd.DataFrame({'year': [2000, 2001]}), 'values': pd.Series([10, 20])},
        {'country': 'Canada', 'data': pd.DataFrame({'year': [2000, 2001]}), 'values': pd.Series([11, 22])},
        {'country': 'Mexico', 'data': pd.DataFrame({'year': [2000, 2001]}), 'values': pd.Series([50, 50])}
    ]
    sdcis = finder._find_sdcis_for_indicator("TEST_INDICATOR", preprocessed_data)
    assert len(sdcis) == 1
    sdci = sdcis[0]
    assert sdci.country_a_id == 1
    assert sdci.country_b_id == 2
    assert sdci.correlation == 1.0
    assert sdci.length == 2


def test_checkpointing_skips_existing_files(finder_test_config, mocker):
    """Verify that the pipeline skips countries with existing output files."""
    finder = ScdiFinder(config=finder_test_config)
    log_spy = mocker.spy(finder.logger, 'info')

    indicator_id = "INDICATOR_A"
    output_dir = os.path.join(finder_test_config.output_path, indicator_id)
    os.makedirs(output_dir, exist_ok=True)
    pd.DataFrame().to_parquet(os.path.join(output_dir, "USA.parquet"))

    finder.run()

    log_spy.assert_any_call("[INDICATOR_A] Skipping 'USA': Output already exists.")


def test_full_run_creates_output(finder_test_config):
    """Test the full pipeline run and verify that output files are created."""
    finder = ScdiFinder(config=finder_test_config)
    finder.run()

    output_file = os.path.join(
        finder_test_config.output_path, "INDICATOR_A", "USA.parquet"
    )
    assert os.path.exists(output_file)

    df = pd.read_parquet(output_file)
    assert not df.empty
    assert 'country_b_id' in df.columns
    assert df['country_b_id'].iloc[0] == 2
