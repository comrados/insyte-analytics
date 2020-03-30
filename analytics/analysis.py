import logging
import os

"""
Base analysis function.
"""

CLASS_NAME = "Analysis"
ANALYSIS_NAME = "analysis"


class Analysis:
    A_ARGS = {"analysis_code": "DEMAND_RESPONSE_BASELINE",
              "analysis_name": "demand-response-baseline",
              "input": "input data description",
              "action": "analysis action description",
              "output": "output data description",
              "parameters": [
                  {"name": "p1", "count": 1, "type": "SELECT", "options": ["o1", "o2", "o3"], "info": "p1 description"},
                  {"name": "p2", "count": 1, "type": "INTEGER", "info": "p2 description"},
                  {"name": "p3", "count": 1, "type": "FLOAT", "info": "p3 description"},
                  {"name": "p4", "count": 1, "type": "DATE", "info": "p4 description"},
                  {"name": "p5", "count": 1, "type": "TIME", "info": "p5 description"},
              ]}

    logger = logging.getLogger(os.path.split(__file__)[1])

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
