"""ss"""

from stages.extract.index import extract_runner  # pylint: disable=import-error
from stages.transform.index import transform_runner  # pylint: disable=import-error
from stages.load.index import load_runner  # pylint: disable=import-error


def etl(etl_input):
    indicators = extract_runner(etl_input)
    indicators = transform_runner(indicators)
    load_runner(indicators)
