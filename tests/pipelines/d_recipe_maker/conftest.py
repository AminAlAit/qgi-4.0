"""TODO"""

from pathlib import Path
import pandas as pd
import pytest
from pipelines.d_recipe_maker.recipe_maker import RecipeMakerConfig


@pytest.fixture(name="recipe_test_config")
def _recipe_test_config_fixture(tmp_path):
    """Creates a RecipeMakerConfig object pointing to temporary test paths."""
    countries_content = "id,name_1\n1,USA\n2,Canada\n3,Mexico"
    countries_path = tmp_path / "countries.csv"
    countries_path.write_text(countries_content)

    return RecipeMakerConfig(
        input_scdi_path=str(tmp_path / "scdi_input"),
        output_recipes_path=str(tmp_path / "recipes_output"),
        events_path=str(tmp_path / "events_input"),
        countries_csv_path=str(countries_path)
    )

@pytest.fixture
def mock_recipe_inputs(recipe_test_config):
    """Creates dummy SCDI and Event files for testing."""
    # Create mock SCDI file
    scdi_dir = Path(recipe_test_config.input_scdi_path) / "POP_GROWTH"
    scdi_dir.mkdir(parents=True, exist_ok=True)
    scdi_df = pd.DataFrame({
        'country_a_id': [1], 'country_b_id': [2],
        'length': [5], 'start_year_a': [2000], 'start_year_b': [2010],
        'correlation': [0.95]
    })
    scdi_df.to_parquet(scdi_dir / "USA.parquet")

    # Create mock Event files
    events_dir = Path(recipe_test_config.events_path)
    events_dir.mkdir(exist_ok=True)
    
    # --- FIXED SECTION ---
    # Event for USA in 2005 (2000 + 5)
    usa_events = ". USA | 2005 | Trade Agreement | Signed new trade deal"
    (events_dir / "USA.txt").write_text(usa_events)

    # Event for Canada in 2015 (2010 + 5)
    can_events = ". Canada | 2015 | Trade Agreement | Ratified trade deal"
    (events_dir / "Canada.txt").write_text(can_events)
