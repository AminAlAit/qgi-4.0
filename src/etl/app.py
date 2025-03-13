"""empty module string"""

from etl_runner.index import etl  # pylint: disable=import-error


etl_input = {
    "indicator_ids": [
        "SP.POP.TOTL",
        "PV.EST"
    ]
}


def run_etl(etl_input):
    etl(etl_input)


run_etl(etl_input)
