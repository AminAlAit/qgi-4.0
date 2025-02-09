

from etl_runner.index import etl


## This is where we read pipeline input data
indicator_ids_list = ["SP.POP.TOTL", "PV.EST"]

dfs = etl(indicator_ids_list)

for i, df in enumerate(dfs):
    df.to_csv(str(i) + ".csv")
