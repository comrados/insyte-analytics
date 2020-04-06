import logging
import os

"""
Base analysis function.
"""

CLASS_NAME = "Analysis"
ANALYSIS_NAME = "analysis"
A_ARGS = {"analysis_code": "ANALYSIS",
          "analysis_name": "analysis",
          "input": "input data description",
          "action": "analysis action description",
          "output": "output data description",
          "mode": "rw",
          "parameters": [
              {"name": "p1", "count": 1, "type": "SELECT", "options": ["o1", "o2", "o3"], "info": "p1 description"},
              {"name": "p2", "count": 1, "type": "INTEGER", "info": "p2 description"},
              {"name": "p3", "count": 1, "type": "FLOAT", "info": "p3 description"},
              {"name": "p4", "count": 1, "type": "DATE", "info": "p4 description"},
              {"name": "p5", "count": 1, "type": "TIME", "info": "p5 description"},
          ]}


class Analysis:
    logger = logging.getLogger(os.path.split(__file__)[1])

    def __init__(self):
        pass

    def analyze(self, parameters, data):
        """
        Do not modify this.
        This method implements analysis cycle.

        :return: analysis result represented as DF
        """
        try:
            p = self._parse_parameters(parameters)
            d = self._preprocess_df(data)
            res = self._analyze(p, d)
            out = self._prepare_for_output(p, d, res)
            return out
        except Exception as err:
            self.logger.error(err)

    def _parse_parameters(self, parameters):
        """
        Check parameter datatypes, quantity, presence etc.

        :param parameters: raw (unchecked) parameters

        :return: dictionary with parsed parameters
        """
        return parameters

    def _analyze(self, p, d):
        """
        Analyze: Main body of analysis

        :param p: parsed analysis parameters
        :param d: preprocessed analysis data

        :return: df with results
        """
        return d

    def _preprocess_df(self, data):
        """
        Preprocess df: replace or remove NaNs etc.

        :param data: raw data (fom DB)

        :return: preprocessed df
        """
        return data

    def _prepare_for_output(self, p, d, res):
        """
        Prepare df for output: rename columns of output df, reshape df, etc.

        dfs must have names starting from either 'val' (for numerics, e.g. 'val1', 'val2', 'value1', 'value2' etc.)
        or 'bool' (for booleans, e.g. 'bool1', 'bool2', 'boolean1', 'boolean2' etc.)

        df must have a timeseries index. If output data is not time series - make dummy datetime index

        :param p:
        :param d:
        :param res:
        :return:
        """
        return res
