

from etl_runner.index import etl  # pylint: disable=import-error


## This is where we read pipeline input data
indicator_ids_list = ["SP.POP.TOTL", "PV.EST"]

indicators = etl(indicator_ids_list)

for i, indicator in enumerate(indicators):
    indicator.data.to_csv(str(i) + ".csv")
