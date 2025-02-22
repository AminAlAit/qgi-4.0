

import numpy as np
import pandas as pd
from scipy.stats import pearsonr
from src.stages.patternize.patternize_utils import get_pattern_fromIndex_matrix_profile, get_pattern_lengths_list
from src.classes.index import Index
from datetime import datetime

def patternize_index(df: pd.DataFrame, correlation_threshold: float, index_name: str, ind: Index):
    results = []
    org, version, main = get_org_version_main(ind)
    countries_in_df = df["Country"].unique().tolist()
    pattern_lengths_list = get_pattern_lengths_list(df)
    
    for country_a in countries_in_df:
        for pattern_length in pattern_lengths_list:
            country_ts = df[df["Country"] == country_a][index_name].values
            if pattern_length > len(country_ts):
                continue
            pattern, from_index, _ = get_pattern_fromIndex_matrix_profile(country_ts, pattern_length)
            start_year_a = int(df[df["Country"] == country_a]["Year"].iloc[from_index])
            index_values_a = ','.join(map(str, pattern))
            creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for country_b in countries_in_df:
                if country_b == country_a:
                    continue
                other_country_df = df[df["Country"] == country_b]
                other_country_ts = other_country_df[index_name].values
                for i in range(len(other_country_ts) - pattern_length + 1):
                    other_pattern = other_country_ts[i: i + pattern_length]
                    if any(np.isnan(x) for x in pattern + other_pattern):
                        continue
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            corr, _ = pearsonr(pattern, other_pattern)
                            corr = round(corr, 5)
                    except Exception as e:
                        print(f"Patternize Error: {e}")
                        continue
                    if corr >= correlation_threshold:
                        start_year_b = int(other_country_df["Year"].iloc[i])
                        index_values_b = ','.join(map(str, other_pattern))
                        result = {
                            "org_fk": org,
                            "main_name_fk": main,
                            "index_name_fk": index_name,
                            "version_fk": version,
                            "country_a": country_a,
                            "country_b": country_b,
                            "pattern_length_fk": pattern_length,
                            "correlation": corr,
                            "start_year_a_fk": start_year_a,
                            "index_values_a": index_values_a,
                            "start_year_b_fk": start_year_b,
                            "index_values_b": index_values_b,
                            "creation_date": creation_date
                        }
                        results.append(result)
    return results