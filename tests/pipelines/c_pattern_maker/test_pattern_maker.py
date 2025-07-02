"""TODO"""

import os
import pandas as pd
from pipelines.c_pattern_maker.patter_maker import PatternMaker


def test_initial_pattern_creation(maker_test_config, mock_scdi_input_files):
    """Verify that SCDIs are correctly aggregated and filtered into base patterns."""
    maker = PatternMaker(config=maker_test_config)
    maker._create_patterns_from_scdis()

    output_file = os.path.join(maker_test_config.output_patterns_path, "USA", "patterns.parquet")
    assert os.path.exists(output_file)

    df = pd.read_parquet(output_file)
    # The pattern with scdi_count=1 is filtered out, so only one pattern remains.
    assert len(df) == 1
    assert 'average_correlation' in df.columns
    assert 'scdi_count' in df.columns
    # The remaining pattern should have a count of 2.
    assert df['scdi_count'].iloc[0] == 2


def test_top_patterns_report_generation(maker_test_config, mock_scdi_input_files):
    """Test the full run and verify the final top patterns report."""
    maker = PatternMaker(config=maker_test_config)
    maker.run() # Run the entire pipeline

    report_path = maker_test_config.top_patterns_report_path
    assert os.path.exists(report_path)

    df = pd.read_parquet(report_path)
    # After aggregation, only one unique pattern remains (USA-Canada, length 3)
    assert len(df) == 1
    assert 'significance' in df.columns
    # The scdi_count should be 2 after the symmetric merge.
    assert df['scdi_count'].max() == 2
