"""TODO"""

import os
import pandas as pd
from pipelines.c_pattern_maker.pattern_maker import PatternMaker


def test_initial_pattern_creation(maker_test_config, mock_scdi_input_files):
    """Verify that SCDIs are correctly aggregated and filtered into base patterns."""
    maker = PatternMaker(config=maker_test_config)
    maker._create_patterns_from_scdis()

    output_file = os.path.join(
        maker_test_config.output_patterns_path, "USA", "patterns.parquet"
    )
    assert os.path.exists(output_file)

    df = pd.read_parquet(output_file)
    # The pattern with scdi_count=1 is filtered out, so only one pattern remains.
    assert len(df) == 1
    assert "average_correlation" in df.columns
    assert "scdi_count" in df.columns
    # The remaining pattern should have a count of 2.
    assert df["scdi_count"].iloc[0] == 2
