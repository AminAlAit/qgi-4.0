"""TODO"""

import pandas as pd
import pytest
from pipelines.b_scdi_finder.scdi_finder import ScdiFinderConfig


@pytest.fixture(name="finder_countries_csv")
def mock_countries_csv_for_finder(tmp_path):
    """Creates a dummy countries.csv file with IDs."""
    csv_content = """id,name_1
1,USA
2,Canada
3,Mexico"""
    csv_path = tmp_path / "countries.csv"
    csv_path.write_text(csv_content)
    return str(csv_path)


@pytest.fixture(name="finder_input_files")
def mock_indicator_files(tmp_path):
    """Creates dummy indicator parquet files to be discovered by the pipeline."""
    input_dir = tmp_path / "input_data"
    input_dir.mkdir()

    # Data for Indicator A
    df_a = pd.DataFrame(
        {
            "country": ["USA", "Canada", "Mexico", "USA", "Canada", "Mexico"],
            "year": [2000, 2000, 2000, 2001, 2001, 2001],
            "value": [10, 11, 50, 20, 22, 50],
        }
    )
    df_a.to_parquet(input_dir / "INDICATOR_A.parquet")

    # Data for Indicator B
    df_b = pd.DataFrame(
        {"country": ["USA", "Canada"], "year": [2010, 2010], "value": [1, 2]}
    )
    df_b.to_parquet(input_dir / "INDICATOR_B.parquet")

    return str(input_dir)


@pytest.fixture(name="finder_test_config")
def scdi_finder_test_config(tmp_path, finder_countries_csv, finder_input_files):
    """Creates a ScdiFinderConfig object pointing to temporary test paths."""
    output_dir = tmp_path / "output_data"
    output_dir.mkdir()

    return ScdiFinderConfig(
        input_path=finder_input_files,
        output_path=str(output_dir),
        countries_csv_path=finder_countries_csv,
        min_length=2,
        correlation_threshold=0.99,
    )
