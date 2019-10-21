import logging


"""
Base analysis function.
"""


class Analysis:
    logger = logging.getLogger('insyte_analytics.analytics.analysis')

    def __init__(self, parameters, data):
        self.parameters = parameters
        self.original_data = data
        self.data = self._preprocess_df()

    def analyze(self):
        self._parse_parameters()

    def _preprocess_df(self):
        pass

    def _parse_parameters(self):
        pass
