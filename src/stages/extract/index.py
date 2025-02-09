import wbgapi as wb
import pandas as pd

def get_indicators_dfs(indicator_ids_list: list[str]) -> list[pd.DataFrame]:
    try:
        indicator_dfs: list[pd.DataFrame] = []
        for indicator_id in indicator_ids_list:
            indicator_dfs.append(wb.data.DataFrame(indicator_id))
        return indicator_dfs
    except Exception as e:
        print(str(e))

def run(indicator_ids_list):
    return get_indicators_dfs(indicator_ids_list)
