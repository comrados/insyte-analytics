import logging
import pandas as pd
from scipy.optimize import minimize
from sklearn.metrics import mean_squared_error

from analytics.analysis import Analysis
import numpy as np

from sklearn.model_selection import TimeSeriesSplit

"""
Time series prediction. Holt-Winters method (triple exponential smoothing)
"""


class PredictionHoltWintersAutoAnalysis(Analysis):
    logger = logging.getLogger('insyte_analytics.analytics.analysis_prediction_holt_winters_auto')

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")

    def analyze(self):
        try:
            super().analyze()
            predicted = self._triple_exponential_smoothing()
            res = self._format_results(predicted)
            return res
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
            self._check_season_length()
            self._check_n_predictions()
            self._check_tolerance()
            self._init_hyperparameters()
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
            self.logger.debug(
                "Wrong parameter 'slength': " + str(self.parameters['season_length'][0]) + " " + str(err))
            raise Exception(
                "Wrong parameter 'slength': " + str(self.parameters['season_length'][0]) + " " + str(err))

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
            self.logger.debug(
                "Wrong parameter 'n_predictions': " + str(self.parameters['n_predictions'][0]) + " " + str(err))
            raise Exception(
                "Wrong parameter 'n_predictions': " + str(self.parameters['n_predictions'][0]) + " " + str(err))

    def _check_tolerance(self):
        """
        Checks 'tolerance' parameter
        """
        try:
            if 1 <= int(self.parameters['tolerance'][0]):
                self.tolerance = float(1 / 10 ** int(self.parameters['tolerance'][0]))
            else:
                raise Exception('Must be grater than or equal 1')

            self.logger.debug("Parsed parameter 'tolerance': " + str(self.tolerance))
        except Exception as err:
            self.logger.debug(
                "Wrong parameter 'tolerance': " + str(self.parameters['tolerance'][0]) + " " + str(err))
            raise Exception(
                "Wrong parameter 'tolerance': " + str(self.parameters['tolerance'][0]) + " " + str(err))

    def _init_hyperparameters(self):
        """
        initialize hyperparameters: alpha, beta, gamma

        :return:
        """

        self.alpha = 0.
        self.beta = 0.
        self.gamma = 0.

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

                continue
            if i >= len(self.series):  # прогнозируем
                m = i - len(self.series) + 1
                result.append((sm + m * tr) + seasonals[i % self.slength])

            else:
                val = self.series[i]
                last_smooth = sm
                sm = self.alpha * (val - seasonals[i % self.slength]) + (1 - self.alpha) * (sm + tr)
                tr = self.beta * (sm - last_smooth) + (1 - self.beta) * tr
                seasonals[i % self.slength] = self.gamma * (val - sm) + (1 - self.gamma) * seasonals[i % self.slength]
                result.append(sm + tr + seasonals[i % self.slength])

            smooth.append(sm)
            trend.append(tr)
            season.append(seasonals[i % self.slength])

        return result

    def _format_results(self, result):
        """
        format results for output

        :param result: unformatted results
        :return: formatted results
        """

        dr1 = self.data.index
        dr2 = pd.date_range(dr1[-1] + (dr1[-1] - dr1[-2]), periods=self.n_predictions)

        idx = dr1.append(dr2)

        return pd.DataFrame(result, idx, ['pred'])

    def _timeseriesCVscore(self, x):
        # вектор ошибок
        errors = []

        # values = data.values
        alpha, beta, gamma = x

        # задаём число фолдов для кросс-валидации
        tscv = TimeSeriesSplit(n_splits=2)

        # идем по фолдам, на каждом обучаем модель, строим прогноз на отложенной выборке и считаем ошибку
        for train, test in tscv.split(values):
            print(len(values[train]), len(values[test]))
            model = HoltWinters(series=values[train], slen=24, alpha=alpha, beta=beta, gamma=gamma, n_preds=len(test))
            model.triple_exponential_smoothing()

            predictions = model.result[-len(test):]
            actual = values[test]
            error = mean_squared_error(predictions, actual)
            errors.append(error)

        # Возвращаем средний квадрат ошибки по вектору ошибок
        return np.mean(np.array(errors))

    def _optimize(self):
        opt = minimize(self._timeseriesCVscore, x0=x, method="TNC", bounds=((0, 1), (0, 1), (0, 1)), tol=1e-3)

        # Из оптимизатора берем оптимальное значение параметров
        alpha_final, beta_final, gamma_final = opt.x