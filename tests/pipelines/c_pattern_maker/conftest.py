"""TODO"""

import os
import pandas as pd
import pytest
from pipelines.c_pattern_maker.pattern_maker import PatternMakerConfig


@pytest.fixture(name="maker_test_config")
def pattern_maker_test_config(tmp_path):
    """Creates a PatternMakerConfig object pointing to temporary test paths."""
    countries_csv_content = "id,name_1\n1,USA\n2,Canada"
    countries_csv_path = tmp_path / "countries.csv"
    countries_csv_path.write_text(countries_csv_content)

    return PatternMakerConfig(
        input_scdi_path=str(tmp_path / "scdi_input"),
        output_patterns_path=str(tmp_path / "patterns_output"),
        countries_csv_path=str(countries_csv_path),
    )


@pytest.fixture
def mock_scdi_input_files(maker_test_config):
    """Creates dummy SCDI parquet files as input for the pipeline."""
    indicator_dir = os.path.join(maker_test_config.input_scdi_path, "INDICATOR_A")
    os.makedirs(indicator_dir)

    # --- MODIFIED DATA ---
    # This DataFrame now contains duplicate rows for the first pattern.
    # This ensures that after aggregation, its 'scdi_count' will be 2,
    # so it will pass the filter and a file will be written.
    df_usa = pd.DataFrame(
        {
            "country_a_id": [1, 1, 1],
            "country_b_id": [2, 2, 2],
            "length": [3, 3, 4],  # First two rows are a duplicate pattern
            "start_year_a": [2000, 2000, 2001],
            "start_year_b": [2005, 2005, 2006],
            "correlation": [0.9, 0.8, 0.95],
        }
    )
    df_usa.to_parquet(os.path.join(indicator_dir, "USA.parquet"))
