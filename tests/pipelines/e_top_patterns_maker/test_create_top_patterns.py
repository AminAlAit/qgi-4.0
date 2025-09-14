"""
Unit and integration tests for the Top Patterns Generator pipeline.
"""

import os
import pandas as pd
import pytest
from src.pipelines.e_top_patterns_maker.create_top_patterns import TopPatternsGenerator


def test_initialization(top_patterns_config):
    """Verify that the generator can be initialized correctly."""
    try:
        TopPatternsGenerator(config=top_patterns_config)
    except Exception as e:
        pytest.fail(f"TopPatternsGenerator initialization failed: {e}")


def test_load_and_process_patterns_merging(top_patterns_config):
    """
    Tests the core logic of loading, creating symmetric counterparts,
    and correctly merging them into a single, unified DataFrame.
    """
    generator = TopPatternsGenerator(config=top_patterns_config)
    processed_df = generator._load_and_process_patterns()

    assert len(processed_df) == 2, "Should merge symmetric pairs back into original count"

    # Find the USA(1)-Canada(2) pattern
    usa_can_pattern = processed_df[
        (processed_df["country_a_id"] == 1) & (processed_df["country_b_id"] == 2)
    ]
    assert not usa_can_pattern.empty, "USA-Canada pattern should exist"

    # The scdi_count should be summed (4 original + 4 symmetric)
    assert usa_can_pattern.iloc[0]["scdi_count"] == 8

    # The average_correlation should be the mean (0.95 and 0.95 -> 0.95)
    assert usa_can_pattern.iloc[0]["average_correlation"] == 0.95

    # The indicator_ids list should be merged and made unique
    expected_ids = sorted(['ID1', 'ID2', 'ID3', 'ID4'])
    assert sorted(usa_can_pattern.iloc[0]["indicator_ids"]) == expected_ids


def test_significance_score_calculation(top_patterns_config):
    """
    Tests the significance score calculation with a known input and expected output.
    """
    generator = TopPatternsGenerator(config=top_patterns_config)

    test_df = pd.DataFrame(
        {"length": [5], "average_correlation": [0.9], "scdi_count": [8]}
    )

    generator._norm_params = {
        "length": {"min": 1, "max": 10},
        "average_correlation": {"min": 0.5, "max": 1.0},
        "scdi_count": {"min": 1, "max": 10},
    }

    scored_df = generator._calculate_significance_score(test_df)

    expected_score = (0.3 * 4 / 9) + (0.4 * 0.8) + (0.3 * 7 / 9)

    assert "SignificanceScore" in scored_df.columns
    assert abs(scored_df.iloc[0]["SignificanceScore"] - expected_score) < 1e-4


def test_full_run_creates_output(top_patterns_config):
    """
    An integration test to verify that a full pipeline run creates the
    expected output file with the correct structure.
    """

    generator = TopPatternsGenerator(config=top_patterns_config)
    result = generator.run()

    assert result.status == "Success"
    assert result.total_patterns_merged == 2
    assert result.top_patterns_saved == 2

    output_path = result.output_file_path
    assert os.path.exists(output_path)

    output_df = pd.read_parquet(output_path)
    assert len(output_df) == 2
    assert "SignificanceScore" in output_df.columns
    assert "indicator_ids" in output_df.columns
    assert "indicator_names" in output_df.columns

    assert (
        output_df.iloc[0]["SignificanceScore"] >= output_df.iloc[1]["SignificanceScore"]
    )
 