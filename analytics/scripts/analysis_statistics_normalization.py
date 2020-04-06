import pandas as pd
from analytics.analysis import Analysis

"""
Normalization
"""

CLASS_NAME = "SatisticsNormalizationAnalysis"
ANALYSIS_NAME = "normalization"
A_ARGS = {"analysis_code": "NORMALIZATION",
          "analysis_name": ANALYSIS_NAME,
          "input": "N time series",
          "action": "Normalizes the data (linear normalization)",
          "output": "N normalized time series",
          "mode": "rw",
          "parameters": [
              {"name": "min_value", "count": 1, "type": "FLOAT", "info": "lower bond"},
              {"max_value": "beta", "count": 1, "type": "FLOAT", "info": "upper bond"}
          ]}


class SatisticsNormalizationAnalysis(Analysis):

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
            results = self._normalize(p, d)
            self.logger.debug("Normalized data:\n\n" + str(results) + "\n")
            return results
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
            min_val = self._check_min_value(parameters)
            max_val = self._check_max_value(parameters)
            if min_val >= max_val:
                raise Exception("'min_value' is greater or equal to 'max_value'")
            else:
                return {'min_value': min_val, 'max_value': max_val}
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_min_value(self, parameters):
        """
        Checks 'min_value' parameter
        """
        try:
            min_value = float(parameters['min_value'][0])
            self.logger.debug("Parsed parameter 'min_value': " + str(min_value))
            return min_value
        except Exception as err:
            self.logger.debug("Parameter 'min_value' can't be parsed: " + str(parameters['min_value']) + ' ' + str(err))

    def _check_max_value(self, parameters):
        """
        Checks 'max_value' parameter
        """
        try:
            max_value = float(parameters['max_value'][0])
            self.logger.debug("Parsed parameter 'max_value': " + str(max_value))
            return max_value
        except Exception as err:
            self.logger.debug("Parameter 'max_value' can't be parsed: " + str(parameters['max_value']) + ' ' + str(err))

    def _normalize(self, p, d):
        """
        Normalizes data in range (min_value, max_value)
        """
        normalized = pd.DataFrame()

        for col in d.columns:
            normalized[col] = self._norm_range(d[col], d[col].min(), d[col].max())
            normalized[col] = normalized[col] * (p['max_value'] - p['min_value']) + p['min_value']

        return normalized

    @staticmethod
    def _norm_range(data, hi, lo):
        """
        range normalization

        :param data: data to normalize
        :param hi: highest threshold value
        :param lo: lowest threshold value
        :return: normalized from 0 to 1 values
        """
        data = data.where(data < lo, other=lo)
        data = data.where(data > hi, other=hi)
        r = hi - lo
        return 1 - (data - lo) / r

    def _prepare_for_output(self, p, d, res):
        """
        Postprocesses DataFrame
        """
        new_names = {col: ('val' + str(i)) for i, col in enumerate(res.columns)}
        res.rename(columns=new_names, inplace=True)
        return res
