"""TODO"""

import os
import pandas as pd
from pipelines.d_recipe_maker.recipe_maker import RecipeMaker


def test_load_events(recipe_test_config, mock_recipe_inputs):
    """Verify that events are loaded and parsed correctly from text files."""
    maker = RecipeMaker(config=recipe_test_config)
    maker._load_events()

    assert 'USA' in maker.events_data
    assert len(maker.events_data['USA']) == 1
    event = maker.events_data['USA'][0]
    assert event.year == 2005
    assert event.event_type == 'Trade Agreement'

def test_get_fitting_events_for_scdi(recipe_test_config, mock_recipe_inputs):
    """Verify the logic that matches an SCDI to historical events."""
    maker = RecipeMaker(config=recipe_test_config)
    maker._load_events()

    # This SCDI row should match the mock events created in the fixture
    scdi_row = pd.Series({
        "country_a_id": 1, "country_b_id": 2,
        "length": 5, "start_year_a": 2000, "start_year_b": 2010
    })

    fitting_events = maker._get_fitting_events_for_scdi(scdi_row)
    assert len(fitting_events) == 1
    assert fitting_events[0] == 'Trade Agreement'

def test_save_or_merge_recipes(recipe_test_config):
    """Test that recipe files are correctly created and merged."""
    maker = RecipeMaker(config=recipe_test_config)
    event_type = "Test Event"
    
    # Create first batch of recipes
    df1 = pd.DataFrame([
        {"indicator_name": "IND_A", "country_name": "USA", "scdi_row_index": 0}
    ])
    maker._save_or_merge_recipe(df1, event_type)
    
    output_file = os.path.join(recipe_test_config.output_recipes_path, "test_event.parquet")
    assert os.path.exists(output_file)

    # Create second batch with one duplicate and one new recipe
    df2 = pd.DataFrame([
        {"indicator_name": "IND_A", "country_name": "USA", "scdi_row_index": 0}, # Duplicate
        {"indicator_name": "IND_B", "country_name": "CAN", "scdi_row_index": 5}, # New
    ])
    maker._save_or_merge_recipe(df2, event_type)

    # Read the final merged file and check contents
    final_df = pd.read_parquet(output_file)
    assert len(final_df) == 2 # Should have deduplicated the rows


def test_full_run_integration(recipe_test_config, mock_recipe_inputs):
    """Test the full pipeline run from mock inputs to final recipe output."""
    maker = RecipeMaker(config=recipe_test_config)
    maker.run()

    # Check that the final recipe file was created
    expected_recipe_file = os.path.join(
        recipe_test_config.output_recipes_path, "trade_agreement.parquet"
    )
    assert os.path.exists(expected_recipe_file)

    # Check the contents of the recipe file
    df = pd.read_parquet(expected_recipe_file)
    assert len(df) == 1
    recipe = df.iloc[0]
    assert recipe['indicator_name'] == 'POP_GROWTH'
    assert recipe['country_name'] == 'USA'
