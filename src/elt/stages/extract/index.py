
import pandas as pd
from stages.extract.wb_extractor import get_indicator  # pylint: disable=import-error


def extract_runner(etl_input):
    return get_indicators_dfs(etl_input)


def get_indicators_dfs(etl_input: dict) -> list[pd.DataFrame]:
    try:
        extracted_indicators = []
        for indicator_id in etl_input["indicator_ids"]:
            ## TODO: needs to be put in a more stable module, where fetching
            ## and retrying are contained and professionally written.
            ## it must return an Indicator object whenever its called with an indicator ID
            indicator = get_indicator(indicator_id)
            extracted_indicators.append(indicator)
        return extracted_indicators
    except Exception as e:
        print("------------")
        print(str(e))
        print("------------")
