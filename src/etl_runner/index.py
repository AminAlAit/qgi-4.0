
from stages.extract.index import extract_runner  # pylint: disable=import-error
from stages.transform.index import transform_runner  # pylint: disable=import-error


def etl(indicator_ids_list):
    indicators = extract_runner(indicator_ids_list)
    indicators = transform_runner(indicators)

    return indicators
