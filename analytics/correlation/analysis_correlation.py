import logging
import pandas as pd
from analytics.analysis import Analysis


class CorrelationAnalysis(Analysis):
    logger = logging.getLogger('insyte_analytics.analytics.analysis_correlation')

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")

    def analyze(self):
        try:
            super().analyze()
            corr_matrix = self.data.corr(method=self.method)
            self.logger.debug("Correlation matrix:\n\n" + str(corr_matrix.values) + "\n")
            results = self._format_results(corr_matrix.values)
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
            self._check_method()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_method(self):
        """
        Checks 'method' parameter
        """
        try:
            self.method = self.parameters['method'][0]
            if self.method not in ['pearson', 'kendall', 'spearman']:
                raise Exception

            self.logger.debug("Parsed parameter 'method': " + str(self.method))
        except Exception as err:
            self.logger.debug("Wrong parameter 'method': " + str(self.method) + " " + str(err))
            raise Exception("Wrong parameter 'method': " + str(self.method) + " " + str(err))

    def _format_results(self, result):
        """
        format results for output, converts matrix to series
        stacks values in top-down left-right order

        :param result: unformatted results
        :return: formatted results
        """
        new_shape = result.shape[0] * result.shape[1]

        idx = pd.date_range('2000-01-01', periods=new_shape)

        result = result.reshape(new_shape)
        return pd.DataFrame(result, idx, ['value'])
