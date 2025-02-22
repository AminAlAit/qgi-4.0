"""empty module string"""

from etl_runner.index import etl  # pylint: disable=import-error


etl_input = {
    "indicator_ids": [
        "SP.POP.TOTL",
        "PV.EST"
    ]
}


def run_etl(etl_input):
    indicators = etl(etl_input)

    for i, indicator in enumerate(indicators):
        indicator.data.to_csv(str(i) + ".csv")

run_etl(etl_input)
