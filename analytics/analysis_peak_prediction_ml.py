import logging
import pandas as pd
from analytics.analysis import Analysis
import datetime
import calendar
import numpy as np


class PeakPredictionMLAnalysis(Analysis):
    logger = logging.getLogger('insyte_analytics.analytics.analysis_peak_prediction')

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")
        self.model_nn = r"models/peak_stats_nn.hdf5"
        self.model_rf = r"models/peak_stats_rf.pkl"

    def analyze(self):
        super().analyze()
        try:
            self._parse_parameters()
            df = self._get_probabs_for_month()
            return df
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))

    def _parse_parameters(self):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters")
        try:
            self._load_model()
            self._check_month()
            self._check_year()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _load_model(self):
        """
        Loads model
        """
        try:
            if self.parameters['model'][0] is 'nn':
                model = self.model_nn
            elif self.parameters['model'][0] is 'rf':
                model = self.model_rf
            else:
                raise Exception("No such model type: " + str(self.parameters['model'][0]))
            self.probabs = self._load_probabs(model)
            self.logger.debug("Model loaded from: " + str(model))
        except Exception as err:
            self.logger.debug("Can't load model: " + str(self.parameters['model'][0]) + " " + str(err))
            raise Exception("Can't load model: " + str(self.parameters['model'][0]) + " " + str(err))

    def _check_month(self):
        """
        Checks 'month' parameter
        """
        try:
            self.month = int(self.parameters['month'][0])

            self.logger.debug("Parsed parameter 'month': " + str(self.month))
        except Exception as err:
            self.logger.debug("Wrong parameter 'month': " + str(self.month) + " " + str(err))
            raise Exception("Wrong parameter 'month': " + str(self.month) + " " + str(err))

    def _check_year(self):
        """
        Checks 'year' parameter
        """
        try:
            self.year = int(self.parameters['year'][0])

            self.logger.debug("Parsed parameter 'year': " + str(self.year))
        except Exception as err:
            self.logger.debug("Wrong parameter 'year': " + str(self.year) + " " + str(err))
            raise Exception("Wrong parameter 'year': " + str(self.year) + " " + str(err))

    def _load_probabs(self, path):
        return pd.read_csv(path, index_col=(0, 1, 2))

    def _get_probabs_for_month(self, nullify_weekends=True):
        """
        Gets probabilities from the model for the whole month

        :param nullify_weekends: nullifies all entries for Sat and Sun
        :return: dataframe with probabilities for each day of the month
        """
        days_in_month = calendar.monthrange(self.year, self.month)[1]
        hours = range(24)

        entries = len(hours) * days_in_month

        p = np.zeros((entries, 1))

        for hour in hours:
            for day in range(1, days_in_month + 1):
                _, week, weekday = datetime.date(self.year, self.month, day).isocalendar()
                if nullify_weekends and weekday in [6, 7]:
                    p[(day - 1) * 24 + hour] = 0
                    continue
                try:
                    p[(day - 1) * 24 + hour] = self.probabs.loc[week].loc[weekday - 1].loc[hour]['prob']
                except:
                    p[(day - 1) * 24 + hour] = 0

        return pd.DataFrame(p, index=pd.date_range(datetime.date(self.year, self.month, 1), periods=entries, freq='1H'),
                            columns=['value'])
