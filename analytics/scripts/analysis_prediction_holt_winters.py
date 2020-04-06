import pandas as pd
from analytics.analysis import Analysis
import numpy as np

"""
Time series prediction. Holt-Winters method (triple exponential smoothing)
"""

CLASS_NAME = "PredictionHoltWintersAnalysis"
ANALYSIS_NAME = "prediction-holt-winters"
A_ARGS = {"analysis_code": "PREDICTION-HOLT-WINTERS",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Predicts future values and builds confidence interval",
          "output": "3 time series with predictions and 2 confidence intervals (lower and upper bonds)",
          "mode": "rw",
          "parameters": [
              {"name": "alpha", "count": 1, "type": "FLOAT", "info": "smoothing hyperparameter"},
              {"name": "beta", "count": 1, "type": "FLOAT", "info": "trend hyperparameter"},
              {"name": "gamma", "count": 1, "type": "FLOAT", "info": "seasonality hyperparameter"},
              {"name": "season_length", "count": 1, "type": "INTEGER", "info": "season length"},
              {"name": "n_predictions", "count": 1, "type": "INTEGER", "info": "number of predictions"}
          ]}


class PredictionHoltWintersAnalysis(Analysis):

    def __init__(self):
        super().__init__()
        self.logger.debug("Initialization")
        self.parameters = None
        self.data = None

    def analyze(self, parameters, data):
        """
        This function was modified for this particular special case only (to much to rewrite)

        :return: analysis result represented as DF
        """
        try:
            self.parameters = parameters
            self._parse_parameters(None)
            self.data = self._preprocess_df(data)
            res = self._analyze(None, None)
            out = self._prepare_for_output(None, None, res)
            return out
        except Exception as err:
            self.logger.error(err)
            raise Exception(str(err))

    def _analyze(self, p, d):
        try:
            super()._analyze(p, d)
            predicted = self._triple_exponential_smoothing()
            return predicted
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))

    def _preprocess_df(self, data):
        """
        Preprocesses DataFrame

        Fills NaN with 0s
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs
            if data is not None:
                if data.empty:
                    raise Exception("Empty DataFrame")
                dat = data.fillna(0.)
            else:
                raise Exception("DataFrame is None")
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
            self._check_season_length()
            self._check_alpha()
            self._check_beta()
            self._check_gamma()
            self._check_n_predictions()
            self._check_scaling_factor()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_season_length(self):
        """
        Checks 'season_length' parameter
        """
        try:
            self.slength = int(self.parameters['season_length'][0])

            self.logger.debug("Parsed parameter 'slength': " + str(self.slength))
        except Exception as err:
            self.logger.error(
                "Wrong parameter 'slength': " + str(self.parameters['season_length'][0]) + " " + str(err))
            raise Exception(
                "Wrong parameter 'slength': " + str(self.parameters['season_length'][0]) + " " + str(err))

    def _check_alpha(self):
        """
        Checks 'alpha' parameter
        """
        try:
            self.alpha = float(self.parameters['alpha'][0])
            if self.alpha > 1 or self.alpha < 0:
                raise Exception('Must be in range [0,1]')

            self.logger.debug("Parsed parameter 'alpha': " + str(self.alpha))
        except Exception as err:
            self.logger.error("Wrong parameter 'alpha': " + str(self.parameters['alpha'][0]) + " " + str(err))
            raise Exception("Wrong parameter 'alpha': " + str(self.parameters['alpha'][0]) + " " + str(err))

    def _check_beta(self):
        """
        Checks 'beta' parameter
        """
        try:
            self.beta = float(self.parameters['beta'][0])
            if self.beta > 1 or self.beta < 0:
                raise Exception('Must be in range [0,1]')

            self.logger.debug("Parsed parameter 'beta': " + str(self.beta))
        except Exception as err:
            self.logger.error("Wrong parameter 'beta': " + str(self.parameters['beta'][0]) + " " + str(err))
            raise Exception("Wrong parameter 'beta': " + str(self.parameters['beta'][0]) + " " + str(err))

    def _check_gamma(self):
        """
        Checks 'gamma' parameter
        """
        try:
            self.gamma = float(self.parameters['gamma'][0])
            if self.gamma > 1 or self.gamma < 0:
                raise Exception('Must be in range [0,1]')

            self.logger.debug("Parsed parameter 'gamma': " + str(self.gamma))
        except Exception as err:
            self.logger.error("Wrong parameter 'gamma': " + str(self.parameters['gamma'][0]) + " " + str(err))
            raise Exception("Wrong parameter 'gamma': " + str(self.parameters['gamma'][0]) + " " + str(err))

    def _check_n_predictions(self):
        """
        Checks 'n_predictions' parameter
        """
        try:
            self.n_predictions = int(self.parameters['n_predictions'][0])
            if self.n_predictions < 0:
                raise Exception('Must be grater than 0')

            self.logger.debug("Parsed parameter 'n_predictions': " + str(self.n_predictions))
        except Exception as err:
            self.logger.error(
                "Wrong parameter 'n_predictions': " + str(self.parameters['n_predictions'][0]) + " " + str(err))
            raise Exception(
                "Wrong parameter 'n_predictions': " + str(self.parameters['n_predictions'][0]) + " " + str(err))

    def _check_scaling_factor(self):
        """
        Checks 'scaling_factor' parameter
        """
        try:
            self.scaling_factor = float(self.parameters['scaling_factor'][0])
            if self.scaling_factor < 0:
                raise Exception('Must be grater than 0')

            self.logger.debug("Parsed parameter 'scaling_factor': " + str(self.scaling_factor))
        except Exception as err:
            self.logger.error(
                "Wrong parameter 'scaling_factor': " + str(self.parameters['scaling_factor'][0]) + " " + str(err))
            raise Exception(
                "Wrong parameter 'scaling_factor': " + str(self.parameters['scaling_factor'][0]) + " " + str(err))

    def _initial_trend(self):
        sum = 0.0
        for i in range(self.slength):
            sum += float(self.series[i + self.slength] - self.series[i]) / self.slength
        return sum / self.slength

    def _initial_seasonal_components(self):
        seasonals = {}
        season_averages = []
        n_seasons = int(len(self.series) / self.slength)
        # вычисляем сезонные средние
        for j in range(n_seasons):
            season_averages.append(
                sum(self.series[self.slength * j:self.slength * j + self.slength]) / float(self.slength))
        # вычисляем начальные значения
        for i in range(self.slength):
            sum_of_vals_over_avg = 0.0
            for j in range(n_seasons):
                sum_of_vals_over_avg += self.series[self.slength * j + i] - season_averages[j]
            seasonals[i] = sum_of_vals_over_avg / n_seasons
        return seasonals

    def _triple_exponential_smoothing(self):
        result = []
        smooth = []
        season = []
        trend = []
        pred_dev = []
        upper_bond = []
        lower_bond = []

        self.series = np.array(self.data[self.data.columns[0]])

        seasonals = self._initial_seasonal_components()

        sm, tr = 0, 0

        for i in range(len(self.series) + self.n_predictions):
            if i == 0:  # инициализируем значения компонент
                sm = self.series[0]
                tr = self._initial_trend()
                result.append(self.series[0])
                smooth.append(sm)
                trend.append(tr)
                season.append(seasonals[i % self.slength])

                pred_dev.append(0)
                upper_bond.append(result[0] + self.scaling_factor * pred_dev[0])
                lower_bond.append(result[0] - self.scaling_factor * pred_dev[0])

                continue
            if i >= len(self.series):  # прогнозируем
                m = i - len(self.series) + 1
                result.append((sm + m * tr) + seasonals[i % self.slength])

                # во время прогноза с каждым шагом увеличиваем неопределенность
                pred_dev.append(pred_dev[-1] * 1.01)

            else:
                val = self.series[i]
                last_smooth = sm
                sm = self.alpha * (val - seasonals[i % self.slength]) + (1 - self.alpha) * (sm + tr)
                tr = self.beta * (sm - last_smooth) + (1 - self.beta) * tr
                seasonals[i % self.slength] = self.gamma * (val - sm) + (1 - self.gamma) * seasonals[i % self.slength]
                result.append(sm + tr + seasonals[i % self.slength])

                # Отклонение рассчитывается в соответствии с алгоритмом Брутлага
                pred_dev.append(self.gamma * np.abs(self.series[i] - result[i]) + (1 - self.gamma) * pred_dev[-1])

            upper_bond.append(result[-1] + self.scaling_factor * pred_dev[-1])

            lower_bond.append(result[-1] - self.scaling_factor * pred_dev[-1])

            smooth.append(sm)
            trend.append(tr)
            season.append(seasonals[i % self.slength])

        return result, lower_bond, upper_bond

    def _prepare_for_output(self, p, d, res):
        """
        format results for output

        :param res: unformatted results
        :return: formatted results
        """
        try:
            dr1 = self.data.index
            dr2 = pd.date_range(dr1[-1] + (dr1[-1] - dr1[-2]), periods=self.n_predictions)

            idx = dr1.append(dr2)

            out = pd.DataFrame(res[0], idx, ['val_pred'])

            out['val_low'] = res[1]
            out['val_up'] = res[2]

            return out
        except Exception as err:
            self.logger.error("Output preparation: " + str(err))
            raise Exception("Output preparation: " + str(err))
