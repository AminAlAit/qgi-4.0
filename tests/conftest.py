"""TODO"""

import pandas as pd
import pytest
from src.etl.domain.indicator import Indicator
import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def sample_etl_input():
    return {
        "indicator_ids": ["SP.POP.TOTL", "PV.EST"],
        "config": {
            "output_format": "parquet",
            "output_path": "/workspaces/qgi-4.0/data/etl_output",
            "file_size_limit": 1000000
        }
    }


@pytest.fixture
def mock_indicator():
    indicator = Indicator()
    indicator.id = "SP.POP.TOTL"
    indicator.name = "Total Population"
    indicator.data = pd.DataFrame({
        "country": ["USA", "CAN"],
        "year": [2020, 2020],
        "value": [331002651, 37742500]
    })
    return indicator
