import pandas as pd
from analytics.analysis import Analysis

"""
Correlation. Covariation matrix calculation.
"""

CLASS_NAME = "AutocorrelationAnalysis"
ANALYSIS_NAME = "autocorrelation"
A_ARGS = {"analysis_code": "AUTOCORRELATION",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Calculates autocorellation series for input data",
          "output": "1 time series (shorter length)",
          "parameters": [
              {"name": "step", "count": 1, "type": "INTEGER", "info": "autocorrelation shifting step"}
          ]}


class AutocorrelationAnalysis(Analysis):

    def __init__(self):
        super().__init__()
        self.logger.debug("Initialization")

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

    def _analyze(self, p, d):
        try:
            super()._analyze(p, d)
            autocorr = [d.autocorr(i) for i in range(0, len(d) - 1, p['step'])]
            self.logger.debug("Autocorrelation values count: " + str(len(autocorr)) + "\n")
            return autocorr
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))

    def _preprocess_df(self, data):
        """
        Preprocesses DataFrame

        Drops row if any of columns has NaN
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs and select only 1st column
            if data is not None:
                dat = data.dropna(how='any')
                dat = dat[dat.columns[0]]
            else:
                dat = None
            self.logger.debug("DataFrame preprocessed")
            return dat
        except Exception as err:
            self.logger.error("Failed to preprocess DataFrame: " + str(err))
            raise Exception("Failed to preprocess DataFrame: " + str(err))

    def _parse_parameters(self, parameters):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters")
        try:
            return {'step': self._check_step(parameters)}
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_step(self, parameters):
        """
        Checks 'step' parameter
        """
        try:
            step = int(parameters['step'][0])
            if step < 1:
                raise Exception("'step' should be positive integer")
            self.logger.debug("Parsed parameter 'step': " + str(step))
            return step
        except Exception as err:
            self.logger.debug("Wrong parameter 'step': " + str(parameters['step']) + " " + str(err))
            raise Exception("Wrong parameter 'step': " + str(parameters['step']) + " " + str(err))

    def _prepare_for_output(self, p, d, res):
        """
        format results for output, converts matrix to series
        stacks values in top-down left-right order

        :return: formatted results
        """

        idx = d.index[:len(d) - 1:p['step']]  # dummy index
        o = pd.DataFrame(res, idx, ['value_autocorrelation'])
        return o.fillna(0.)
