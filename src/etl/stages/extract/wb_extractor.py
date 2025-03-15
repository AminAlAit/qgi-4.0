import wbgapi as wb
from domain.indicator import Indicator  # pylint: disable=import-error


## TODO: Indicators are linked from ETL pipeline to patternizer pipeline only via string IDs 
## in the config.yaml file. Find a way to link them via the Indicator class.
def get_indicator(indicator_id):
    """
    Example output:
    id:                   SP.POP.TOTL
    data columns:         ['YR1960', 'YR1961', 'YR1962', 'YR1963', 'YR1964', 'YR1965', 'YR1966',
                           'YR1967', 'YR1968', 'YR1969', 'YR1970', 'YR1971', 'YR1972', 'YR1973',
                           'YR1974', 'YR1975', 'YR1976', 'YR1977', 'YR1978', 'YR1979', 'YR1980',
                           'YR1981', 'YR1982', 'YR1983', 'YR1984', 'YR1985', 'YR1986', 'YR1987',
                           'YR1988', 'YR1989', 'YR1990', 'YR1991', 'YR1992', 'YR1993', 'YR1994',
                           'YR1995', 'YR1996', 'YR1997', 'YR1998', 'YR1999', 'YR2000', 'YR2001',
                           'YR2002', 'YR2003', 'YR2004', 'YR2005', 'YR2006', 'YR2007', 'YR2008',
                           'YR2009', 'YR2010', 'YR2011', 'YR2012', 'YR2013', 'YR2014', 'YR2015',
                           'YR2016', 'YR2017', 'YR2018', 'YR2019', 'YR2020', 'YR2021', 'YR2022',
                           'YR2023']
    data rows:            266
    name:                 Population, total
    concept:              Series
    license_type:         CC BY-4.0
    license_url:          https://datacatalog.worldbank.org/public-licenses#cc-by
    long_definition:      Total population is based on the de facto definition of population, 
                          which counts all residents regardless of legal status or citizenship. 
                          The values shown are midyear estimates.
    othernotes:           
    periodicity:          Annual
    related_source_links: 
    source:               (1) United Nations Population Division. World Population Prospects:
                          2024 Revision; (2) Statistical databases and publications from national
                          statistical offices; (3) Eurostat: Demographic Statistics;
                          (4) United Nations Statistics Division. Population and Vital Statistics
                          Report (various years).
    topic:                Health: Population: Structure
    """
    indicator          = Indicator()
    indicator_metadata = wb.series.metadata.get(indicator_id)
    indicator.id       = indicator_id
    indicator.data     = wb.data.DataFrame(indicator_id)

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
