import pandas as pd

class Pattern:
    def __init__(self):
        self.id = ""
        self.indicator_id = ""
        self.country_a = ""
        self.country_b = ""
        self.start_year = 0
        self.end_year = 0
        self.length = 0
        self.correlation = 0.0
        self.data_segment_a = pd.Series()
        self.data_segment_b = pd.Series()
