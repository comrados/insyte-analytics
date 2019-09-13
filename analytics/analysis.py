import logging
import pandas as pd


class Analysis:
    """
    Base analysis function.
    """

    logger = logging.getLogger('insyte_analytics.analytics.analysis')

    def __init__(self, parameters, data):
        self.parameters = parameters
        self.original_data = data
        self.data = self._preprocess_df()
        pass

    def analyze(self):
        pass

    def _preprocess_df(self):
        """
        Preprocesses DataFrame
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs
            if self.original_data is not None:
                data = self.original_data.fillna(0.)
            else:
                data = None
            self.logger.debug("DataFrame preprocessed")
            return data
        except Exception as err:
            self.logger.error("Failed to preprocess DataFrame: " + str(err))
            raise Exception("Failed to preprocess DataFrame: " + str(err))
