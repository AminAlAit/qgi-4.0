"""
Pytest fixtures for testing the Top Patterns Generator pipeline.
"""
import os
import pytest
import pandas as pd
import yaml
from src.pipelines.e_top_patterns_maker.create_top_patterns import TopPatternsConfig

@pytest.fixture(scope="module")
def mock_patterns_dir(tmp_path_factory):
    """
    Creates a temporary directory structure with mock pattern parquet files.
    This simulates the output from the `c_pattern_maker` pipeline.
    """
    # Create a base temporary directory for this test session
    base_dir = tmp_path_factory.mktemp("pattern_data")
    
    # Define mock data
    # Pattern 1: A strong, multi-SCDI pattern
    usa_patterns = pd.DataFrame({
        'country_a_id': [1], 'country_b_id': [2], 'length': [5],
        'start_year_a': [2010], 'start_year_b': [2011],
        'average_correlation': [0.95], 'scdi_count': [4]
    })
    
    # Pattern 2: A weaker, but still valid pattern
    mexico_patterns = pd.DataFrame({
        'country_a_id': [3], 'country_b_id': [1], 'length': [3],
        'start_year_a': [2015], 'start_year_b': [2015],
        'average_correlation': [0.85], 'scdi_count': [2]
    })
    
    # Create the directory structure and save the files
    usa_dir = base_dir / "USA"
    mexico_dir = base_dir / "Mexico"
    os.makedirs(usa_dir, exist_ok=True)
    os.makedirs(mexico_dir, exist_ok=True)
    
    usa_patterns.to_parquet(usa_dir / "patterns.parquet")
    mexico_patterns.to_parquet(mexico_dir / "patterns.parquet")
    
    return str(base_dir)


@pytest.fixture(scope="module")
def top_patterns_config(mock_patterns_dir, tmp_path_factory):
    """
    Creates a TopPatternsConfig object pointing to the temporary test directories
    and using a fixed, predictable configuration for testing.
    """
    output_dir = tmp_path_factory.mktemp("top_patterns_output")
    
    config_dict = {
        'input_patterns_path': mock_patterns_dir,
        'output_path': str(output_dir),
        'min_correlation_threshold': 0.70,
        'top_n_patterns': 10,
        'normalization_params': {
            'dynamic': True,
        },
        'score_weights': {
            'length': 0.30,
            'average_correlation': 0.40,
            'scdi_count': 0.30
        }
    }
    return TopPatternsConfig(**config_dict)
