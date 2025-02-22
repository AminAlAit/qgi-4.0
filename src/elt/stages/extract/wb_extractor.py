import wbgapi as wb
from domain.indicator import Indicator  # pylint: disable=import-error

def get_indicator(id):
    indicator                      = Indicator()

    indicator_metadata             = wb.series.metadata.get(id)

    indicator.id                   = id

    indicator.data                 = wb.data.DataFrame(id)

    if "IndicatorName" in list(indicator_metadata.metadata.keys()):
        indicator.name = indicator_metadata.metadata["IndicatorName"]
    else:
        indicator.name = ""

    indicator.concept = indicator_metadata.concept

    if "License_Type" in list(indicator_metadata.metadata.keys()):
        indicator.license_type = indicator_metadata.metadata["License_Type"]
    else:
        indicator.license_type = ""

    if "License_URL" in list(indicator_metadata.metadata.keys()):
        indicator.license_url = indicator_metadata.metadata["License_URL"]
    else:
        indicator.license_url = ""

    if "Longdefinition" in list(indicator_metadata.metadata.keys()):
        indicator.long_definition = indicator_metadata.metadata["Longdefinition"]
    else:
        indicator.long_definition = ""

    if "Othernotes" in list(indicator_metadata.metadata.keys()):
        indicator.othernotes = indicator_metadata.metadata["Othernotes"]
    else:
        indicator.othernotes = ""

    if "Periodicity" in list(indicator_metadata.metadata.keys()):
        indicator.periodicity = indicator_metadata.metadata["Periodicity"]
    else:
        indicator.periodicity = ""

    if "Relatedsourcelinks" in list(indicator_metadata.metadata.keys()):
        indicator.related_source_links = indicator_metadata.metadata["Relatedsourcelinks"]
    else:
        indicator.related_source_links = ""

    if "Source" in list(indicator_metadata.metadata.keys()):
        indicator.source = indicator_metadata.metadata["Source"]
    else:
        indicator.source = ""

    if "Topic" in list(indicator_metadata.metadata.keys()):
        indicator.topic = indicator_metadata.metadata["Topic"]
    else:
        indicator.topic = ""

    return indicator


## TODO add more functionalities here: calculate minimum and maximum years per indicator data,
## how many countries does it cover, -- or maybe we add these details in Transform... 
## more ideas on what to add: value/nan ratio, ask deepseek for other ideas that might be useful down the line
