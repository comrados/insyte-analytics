import pandas as pd
from analytics.analysis import Analysis

"""
Correlation. Covariation matrix calculation.
"""

CLASS_NAME = "CorrelationAnalysis"
ANALYSIS_NAME = "correlation"
A_ARGS = {"analysis_code": "CORRELATION",
          "analysis_name": ANALYSIS_NAME,
          "input": "Data of arbitrary dimensionality (N time series)",
          "action": "Calculates NxN correlation matrix based on the selected calculation method",
          "output": "NxN covariation matrix represented as 1 time series of NxN length",
          "mode": "rw",
          "parameters": [
              {"name": "method", "count": 1, "type": "SELECT", "options": ["pearson", "kendall", "spearman"],
               "info": "pearson - Pearson correlation coefficient, "
                       "kendall - Kendall rank correlation coefficient, "
                       "spearman - Spearman's rank correlation coefficient"}
          ]}


class CorrelationAnalysis(Analysis):

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
            corr_matrix = d.corr(method=p['method'])
            self.logger.debug("Correlation matrix:\n\n" + str(corr_matrix.values) + "\n")
            return corr_matrix
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
            # Fill NaNs
            if data is not None:
                dat = data.dropna(how='any')
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
            return {'method': self._check_method(parameters)}
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_method(self, parameters):
        """
        Checks 'method' parameter
        """
        try:
            method = parameters['method'][0]
            if method not in ['pearson', 'kendall', 'spearman']:
                raise Exception
            self.logger.debug("Parsed parameter 'method': " + str(method))
            return method
        except Exception as err:
            self.logger.debug("Wrong parameter 'method': " + str(parameters['method']) + " " + str(err))
            raise Exception("Wrong parameter 'method': " + str(parameters['method']) + " " + str(err))

    def _prepare_for_output(self, p, d, res):
        """
        format results for output, converts matrix to series
        stacks values in top-down left-right order

        :return: formatted results
        """
        result = res.values  # get numpy array
        new_shape = result.shape[0] * result.shape[1]

        idx = pd.date_range('2000-01-01', periods=new_shape)  # dummy index

        result = result.reshape(new_shape)
        return pd.DataFrame(result, idx, ['value_correlation'])
