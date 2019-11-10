import logging
import pandas as pd
from analytics.analysis import Analysis
import numpy as np
from analytics import utils
import pickle


"""
Peak prediction. Machine learning method
"""


class PeakPredictionMLAnalysis(Analysis):
    logger = logging.getLogger('insyte_analytics.analytics.analysis_peak_prediction')

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")
        self.model_nn = r"models/peak_stats_nn.hdf5"
        self.model_rf = r"models/peak_stats_rf.pkl"

    def analyze(self):
        try:
            super().analyze()
            results = self._predict()
            self.logger.debug("Predicted probabilities:\n\n" + str(results) + "\n")
            df = self._format_results(results)
            self.logger.debug("Predicted probabilities:\n\n" + str(df) + "\n")
            return df
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))

    def _preprocess_df(self):
        """
        Preprocesses DataFrame

        Fills NaN with 0s
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

    def _parse_parameters(self):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters")
        try:
            self._load_model()
            self._check_pred_parameters_lengths()
            self._refactor_parameters()
            self._normalize_parameters()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _load_model(self):
        """
        Loads model
        """
        try:
            if self.parameters['model'][0] == 'nn':
                # keras init is here because it blocks file-logging otherwise
                from keras.models import load_model

                model = self.model_nn
                self.model_type = 'nn'
                self.model = load_model(model)
            elif self.parameters['model'][0] == 'rf':
                model = self.model_rf
                self.model_type = 'rf'
                self.model = pickle.load(open(model, "rb"))
            else:
                self.logger.error("No such model type: " + str(self.parameters['model'][0]))
                raise Exception("No such model type: " + str(self.parameters['model'][0]))
            self.parameters.pop('model')
            self.logger.debug("Model loaded from: " + str(model))
        except Exception as err:
            self.logger.debug("Can't load model: " + str(self.parameters['model'][0]) + " " + str(err))
            raise Exception("Can't load model: " + str(self.parameters['model'][0]) + " " + str(err))

    def _check_pred_parameters_lengths(self):
        """
        Checks if parameter lengths are equal
        """
        lsd = {k: len(v) for k, v in self.parameters.items()}
        ls = [len(v) for k, v in self.parameters.items()]

        length = ls[0]
        for i in ls:
            if i != length:
                self.logger.error('Parameters lengths must be equal: ' + str(lsd))
                raise Exception('Parameters lengths must be equal: ' + str(lsd))
        self.logger.debug("Parameters lengths: " + str(lsd))

    def _refactor_parameters(self):
        """
        Reformats 'date', 'sunrise', 'sunset', 'daylength', 'temperature', 'pressure', 'humidity', 'windspeed' parameters
        """
        self.refactored = pd.DataFrame()

        self.date = [utils.string_to_date(i) for i in self.parameters['date']]
        self.refactored['sunrise'] = [utils.string_to_time(i) for i in self.parameters['sunrise']]
        self.refactored['sunset'] = [utils.string_to_time(i) for i in self.parameters['sunset']]
        self.refactored['daylength'] = [utils.string_to_time(i) for i in self.parameters['daylength']]
        self.refactored['temperature'] = [float(i) for i in self.parameters['temperature']]
        self.refactored['pressure'] = [float(i) for i in self.parameters['pressure']]
        self.refactored['humidity'] = [float(i) for i in self.parameters['humidity']]
        self.refactored['windspeed'] = [float(i) for i in self.parameters['windspeed']]

        self.refactored['weekday'] = [i.weekday() for i in self.date]
        self.refactored['week'] = [i.isocalendar()[1] for i in self.date]
        self.refactored['month'] = [i.month for i in self.date]

        self.logger.debug("Refactored data:\n\n" + str(self.refactored) + "\n")

    def _normalize_parameters(self):
        """
        Normalizes 'date', 'sunrise', 'sunset', 'daylength', 'temperature', 'pressure', 'humidity', 'windspeed' parameters
        """
        self.normalized = pd.DataFrame()

        self.normalized['sunrise_norm'] = self.refactored['sunrise'].apply(
            lambda x: (x.minute + x.hour * 60) / (60 * 24 - 1))
        self.normalized['sunset_norm'] = self.refactored['sunset'].apply(
            lambda x: (x.minute + x.hour * 60) / (60 * 24 - 1))
        self.normalized['daylength_norm'] = self.refactored['daylength'].apply(
            lambda x: (x.minute + x.hour * 60) / (60 * 24 - 1))
        self.normalized["temp_norm_-50_50"] = self._norm_range(self.refactored["temperature"], -50, 50)
        self.normalized["press_norm_700_800"] = self._norm_range(self.refactored["pressure"], 700, 800)
        self.normalized["hum_norm_0_100"] = self._norm_range(self.refactored["humidity"], 0, 100)
        self.normalized["wind_norm_0_25"] = self._norm_range(self.refactored["windspeed"], 0, 25)
        self.normalized["weekday_norm"] = self._norm_range(self.refactored["weekday"], 0, 6)
        self.normalized["week_norm"] = self._norm_range(self.refactored["week"], 1, 53)
        self.normalized["month_norm"] = self._norm_range(self.refactored["month"], 1, 12)

        self.logger.debug("Normalized data:\n\n" + str(self.normalized) + "\n")

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

    def _predict(self):
        """
        Gets probabilities from the model for given data

        :return: numpy array with predictions
        """
        if self.model_type == 'nn':
            return self.model.predict(np.array(self.normalized))
        elif self.model_type == 'rf':
            return self.model.predict_proba(np.array(self.normalized))
        else:
            self.logger.error("No such model type: " + str(self.model_type))
            raise Exception("No such model type: " + str(self.model_type))

    def _format_results(self, result):
        """
        format results for output

        :param result: unformatted results
        :return: formatted results
        """
        idx = pd.date_range(self.date[0], freq='1H', periods=24)
        for i in range(1, len(self.date)):
            dr = pd.date_range(self.date[i], periods=24, freq='1H')
            idx = idx.append(dr)

        result = result.reshape(result.shape[0] * result.shape[1])
        return pd.DataFrame(result, idx, ['value'])
