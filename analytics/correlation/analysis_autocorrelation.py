import logging
import pandas as pd
from analytics.analysis import Analysis

"""
Correlation. Covariation matrix calculation.
"""


class AutocorrelationAnalysis(Analysis):
    A_ARGS = {"analysis_code": "AUTOCORRELATION",
              "analysis_name": "autocorrelation",
              "input": "1 time series",
              "action": "Calculates autocorellation series for input data",
              "output": "1 time series (shorter length)",
              "parameters": [
                  {"name": "step", "count": 1, "type": "INTEGER", "info": "autocorrelation shifting step"}
              ]}

    logger = logging.getLogger('insyte_analytics.analytics.analysis_autocorrelation')

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")

    def analyze(self):
        try:
            super().analyze()
            autocorr = [self.data.autocorr(i) for i in range(0, len(self.data) - 1, self.step)]
            self.logger.debug("Autocorrelation values count: " + str(len(autocorr)) + "\n")
            results = self._format_results(autocorr)
            return results
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))

    def _preprocess_df(self):
        """
        Preprocesses DataFrame

        Drops row if any of columns has NaN
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs
            if self.original_data is not None:
                data = self.original_data.dropna(how='any')
                data = data[data.columns[0]]
            else:
                data = None
            self.logger.debug("DataFrame preprocessed")
            return data
        except Exception as err:
            self.logger.error("Failed to preprocess DataFrame: " + str(err))
            raise Exception("Failed to preprocess DataFrame: " + str(err))

    def _parse_parameters(self):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters")
        try:
            self._check_step()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_step(self):
        """
        Checks 'step' parameter
        """
        try:
            self.step = int(self.parameters['step'][0])
            if self.step < 1:
                raise Exception("'step' should be positive integer")

            self.logger.debug("Parsed parameter 'step': " + str(self.step))
        except Exception as err:
            self.logger.debug("Wrong parameter 'step': " + str(self.step) + " " + str(err))
            raise Exception("Wrong parameter 'step': " + str(self.step) + " " + str(err))

    def _format_results(self, autocorr):
        """
        format results for output, converts matrix to series
        stacks values in top-down left-right order

        :param autocorr: unformatted results
        :return: formatted results
        """

        idx = self.data.index[:len(self.data) - 1:self.step]

        return pd.DataFrame(autocorr, idx, ['autocorrelation'])
