import logging
import pandas as pd
from analytics.analysis import Analysis


"""
Normalization
"""


class SatisticsNormalizationAnalysis(Analysis):
    logger = logging.getLogger('insyte_analytics.analytics.analysis_normalization')

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")

    def analyze(self):
        try:
            super().analyze()
            results = self._normalize()
            self.logger.debug("Normalized data:\n\n" + str(results) + "\n")
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
            self._check_min_value()
            self._check_max_value()
            self._check_values()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_min_value(self):
        """
        Checks 'min_value' parameter
        """
        try:
            self.min_value = float(self.parameters['min_value'][0])
            self.logger.debug("Parsed parameter 'min_value': " + str(self.min_value))
        except Exception as err:
            self.logger.debug("Parameter 'min_value' can't be parsed: " + str(err))

    def _check_max_value(self):
        """
        Checks 'max_value' parameter
        """
        try:
            self.max_value = float(self.parameters['max_value'][0])
            self.logger.debug("Parsed parameter 'max_value': " + str(self.max_value))
        except Exception as err:
            self.logger.debug("Parameter 'max_value' can't be parsed: " + str(err))

    def _check_values(self):
        if self.min_value >= self.max_value:
            raise Exception("'min_value' is greater or equal to 'max_value'")

    def _normalize(self):
        """
        Normalizes data in range (min_value, max_value)
        """
        normalized = pd.DataFrame()

        for col in self.data.columns:
            normalized[col] = self._norm_range(self.data[col], self.data[col].min(), self.data[col].max())
            normalized[col] = normalized[col] * (self.max_value-self.min_value) + self.min_value

        return normalized

    def _norm_range(self, data, hi, lo):
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
