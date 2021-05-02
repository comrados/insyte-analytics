import logging
import os
import datetime
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error
from analytics.analysis import Analysis

"""
Generate CLD forecast function.
"""

CLASS_NAME = "analysisPredictionHW"
ANALYSIS_NAME = "analysis-prediction-hw"
A_ARGS = {"analysis_code": "ANALYSISPREDICTIONHW",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Generate forecast with the use Holt-Winters method",
          "output": "1 time series (on target day)",
          "inputs_count": 1,
          "outputs_count": 1,
          "inputs_outputs_always_same_count": True,
          "mode": "rw",
          "parameters": [
              {"name": "target_day", "count": 1, "type": "DATE", "info": "target day for analysis"},
          ]}


class HoltWinters:
    def __init__(self, series, slen, alpha, beta, gamma, n_preds, scaling_factor=1.96):
        self.series = series  # исходный временной ряд
        self.slen = slen  # длина сезона
        self.alpha = alpha  # коэффициенты модели Хольта-Винтерса
        self.beta = beta
        self.gamma = gamma
        self.n_preds = n_preds  # n_preds - горизонт предсказаний
        self.scaling_factor = scaling_factor  # задаёт ширину доверительного интервала по Брутлагу (обычно принимает значения от 2 до 3)

    def initial_trend(self):
        sum = 0.0
        for i in range(self.slen):
            sum += float(self.series[i + self.slen] - self.series[i]) / self.slen
        return sum / self.slen

    def initial_seasonal_components(self):
        seasonals = {}
        season_averages = []

        n_seasons = int(len(self.series) / self.slen)
        for j in range(n_seasons):  # вычисляем сезонные средние
            season_averages.append(sum(self.series[self.slen * j:self.slen * j + self.slen]) / float(self.slen))
        for i in range(self.slen):  # вычисляем начальные значения
            sum_of_vals_over_avg = 0.0
            for j in range(n_seasons):
                sum_of_vals_over_avg += self.series[self.slen * j + i] - season_averages[j]
            seasonals[i] = sum_of_vals_over_avg / n_seasons
        return seasonals

    def triple_exponential_smoothing(self):
        self.result = []
        self.Smooth = []
        self.Season = []
        self.Trend = []
        self.PredictedDeviation = []
        self.UpperBond = []
        self.LowerBond = []

        seasonals = self.initial_seasonal_components()

        for i in range(len(self.series) + self.n_preds):
            if i == 0:  # инициализируем значения компонент
                smooth = self.series[0]
                trend = self.initial_trend()
                self.result.append(self.series[0])
                self.Smooth.append(smooth)
                self.Trend.append(trend)
                self.Season.append(seasonals[i % self.slen])
                self.PredictedDeviation.append(0)
                self.UpperBond.append(self.result[0] + self.scaling_factor * self.PredictedDeviation[0])
                self.LowerBond.append(self.result[0] - self.scaling_factor * self.PredictedDeviation[0])
                continue
            if i >= len(self.series):  # прогнозируем
                m = i - len(self.series) + 1
                self.result.append((smooth + m * trend) + seasonals[i % self.slen])
                self.PredictedDeviation.append(
                    self.PredictedDeviation[-1] * 1.01)  # во время прогноза с каждым шагом увеличиваем неопределенность
            else:
                val = self.series[i]
                last_smooth, smooth = smooth, self.alpha * (val - seasonals[i % self.slen]) + (1 - self.alpha) * (
                            smooth + trend)
                trend = self.beta * (smooth - last_smooth) + (1 - self.beta) * trend
                seasonals[i % self.slen] = self.gamma * (val - smooth) + (1 - self.gamma) * seasonals[i % self.slen]
                self.result.append(smooth + trend + seasonals[i % self.slen])
                # Отклонение рассчитывается в соответствии с алгоритмом Брутлага
                self.PredictedDeviation.append(
                    self.gamma * np.abs(self.series[i] - self.result[i]) + (1 - self.gamma) * self.PredictedDeviation[
                        -1])
            self.UpperBond.append(self.result[-1] + self.scaling_factor * self.PredictedDeviation[-1])
            self.LowerBond.append(self.result[-1] - self.scaling_factor * self.PredictedDeviation[-1])
            self.Smooth.append(smooth)
            self.Trend.append(trend)
            self.Season.append(seasonals[i % self.slen])

class analysisPredictionHW(Analysis):
    logger = logging.getLogger(os.path.split(__file__)[1])

    def __init__(self):
        super().__init__()
        self.utc = 0
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
            # print("Входные данные:")
            # print(data)
            res = self._analyze(p, d)
            # print(res)
            res = res.astype(float)
            # print("Результат программы:")
            # print(res.round(0))
            # res = self._prepare_for_output(p, d, res)

            # pd.options.display.max_columns = 100
            return res.round(0)
        except Exception as err:
            self.logger.error(err)
            raise Exception(str(err))

    def _parse_parameters(self, parameters):
        """
        Check parameter datatypes, quantity, presence etc.

        :param parameters: raw (unchecked) parameters

        :return: dictionary with parsed parameters
        """
        self.logger.debug("Parsing parameters")
        try:
            pn = { "target_day": parameters['target_day'][0]}
            return pn

        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _analyze(self, p, d):
        """
        Analyze: Main body of analysis

        :param p: parsed analysis parameters
        :param d: preprocessed analysis data

        :return: df with results
        """
        self.logger.debug("Start analyze")
        try:
            date_list = self.create_unique_dates(self.separate_dt(d))
            # print(date_list)
            return self.run_HW(p, d, date_list[1])

        except Exception as err:
            self.logger.error("Error in _analyze: " + str(err))
            raise Exception("Error in _analyze: " + str(err))

    def _preprocess_df(self, data):
        """
        Preprocess df: replace or remove NaNs etc.

        :param data: raw data (fom DB)

        :return: preprocessed df
        """
        data.reset_index(inplace=True)
        data.columns = ['date_time', 'E_load_Wh']
        format_out = '%Y-%m-%d %H:%M:%S'
        # format_in = '%Y-%m-%d %H:%M:%S+00:00'
        data['date_time'] = data['date_time'].apply(lambda x: x.strftime(format_out))
        data['date_time'] = pd.to_datetime(data['date_time'])
        # data.set_index("date_time", inplace=True)
        data = self._preprocess_df_power(data)
        return data

    def _preprocess_df_power(self, data):
        """
        Convert DataFrame to Power/hour
        """
        try:
            # Fill NaNs
            if data is not None:
                if data.empty:
                    raise Exception("Empty DataFrame")
                ndf = data.groupby(pd.Grouper(key="date_time", freq="1H")).count()
                ndf.columns = ['count_val']
                ndf['sum_val'] = data.groupby(pd.Grouper(key="date_time", freq="1H")).sum()
                ndf['sum_power'] = ndf.sum_val / ndf.count_val
                # ndf.reset_index(inplace=True)
                ndf.rename(columns={'sum_power': 'E_load_Wh'},
                           inplace=True)
                return ndf[['E_load_Wh']]
            else:
                raise Exception("DataFrame is None")
        except Exception as err:
            self.logger.error("Failed to preprocess DataFrame: " + str(err))
            raise Exception("Failed to preprocess DataFrame: " + str(err))

    def run_HW(self, p, df, day_list):
        """ Forecasting LOAD by Holt Winters Method """
        try:
            df['val_hw'] = 0
            alpha_final = 0
            beta_final = 0
            gamma_final = 0
            e_list = []
            N = len(df.loc[df.index.date == day_list[len(day_list) - 2]])
            format_out = '%Y-%m-%d'
            target_day = datetime.datetime.strptime(p['target_day'], format_out)
            max_target_day = day_list[len(day_list) - 1] + datetime.timedelta(days=1)
            if (target_day.date() > max_target_day):
                self.logger.error("Target day outside of analysis")
                return False
            if (target_day.date() < max_target_day):
                target_k = day_list.index(target_day.date())
            else:
                if (target_day.date() == max_target_day):
                    target_k = len(day_list)

            if (len(day_list) >= 14):
                copy_from = 2 * 7   # start after this day
            if (len(day_list) >= 7):
                copy_from = 7   # start after this day
            delta_target_day = len(day_list) - (max_target_day - target_day.date()).days
            if (len(day_list) < 7 or delta_target_day < 7):
                df['val_hw'] = df['E_load_Wh']
                df.reset_index(inplace=True)
                df['date_time'] = df.date_time + datetime.timedelta(days=1)
                df.set_index('date_time', inplace=True)
                return df.loc[df.index.date == target_day.date()][['val_hw']]
            for k in range(copy_from, len(day_list)+1):  # why 508 instead of 522?
                data = df['E_load_Wh'][(k - copy_from) * N:(k - 1) * N]

                if (k % 7 == 0):  # ones a week we should adapt parameters
                    x = [alpha_final, beta_final, gamma_final]
                    # TNC - Truncated Newton conjugate gradient
                    opt = minimize(self.timeseriesCVscore, x0=x, args=(data, N), method="TNC", bounds=((0, 1), (0, 1), (0, 1)))  # Минимизируем функцию потерь с ограничениями на параметры
                    alpha_final, beta_final, gamma_final = opt.x  # Из оптимизатора берем оптимальное значение параметров
                    e_list.append(alpha_final)

                model = HoltWinters(data, slen=N, alpha=alpha_final, beta=beta_final, gamma=gamma_final, n_preds=N,
                                    scaling_factor=2.56)
                model.triple_exponential_smoothing()
                copy_to_data = df[df['date'].isin([list(day_list)[k-2]])].copy()[['date', 'time']]
                copy_to_data.reset_index(inplace=True)
                copy_to_data['date_time'] = copy_to_data.date_time + datetime.timedelta(days=2)
                copy_to_data.set_index('date_time', inplace=True)

                for j in range(len(copy_to_data)):
                    l = copy_to_data.index[j]
                    if (model.result[(copy_from - 1) * N + j] > 0):
                        copy_to_data.loc[l, 'val_hw'] = model.result[(copy_from - 1) * N + j]

                if (target_k == k):
                    return copy_to_data.drop(columns=['date', 'time'])
                    # print(f'res{copy_to_data.loc[copy_to_data.index.date == target_day]}')

            # print(len(model.result))
            # print(df)

        except Exception as err:
            self.logger.error("Error in run_HW: " + str(err))
            raise Exception("Error in run_HW: " + str(err))


    def timeseriesCVscore(self, x, data, N):  # is called inside the run_HW method
        try:
            errors = []  # вектор ошибок
            values = data.values
            alpha, beta, gamma = x
            tscv = TimeSeriesSplit(n_splits=2)  # задаём число фолдов для кросс-валидации
            for train, test in tscv.split(
                    values):  # идем по фолдам, на каждом обучаем модель, строим прогноз на отложенной выборке и считаем ошибку
                model = HoltWinters(series=values[train], slen=N, alpha=alpha, beta=beta, gamma=gamma, n_preds=len(test))
                model.triple_exponential_smoothing()
                predictions = model.result[-len(test):]
                actual = values[test]
                error = mean_squared_error(predictions, actual)
                errors.append(error)
            return np.mean(np.array(errors))  # Возвращаем средний квадрат ошибки по вектору ошибок
        except Exception as err:
            self.logger.error("Error in timeseriesCVscore: " + str(err))
            raise Exception("Error in timeseriesCVscore: " + str(err))

    # Вспомогательные функции
    def separate_dt(self, df):
        df.insert(0, "date", df.index.date)  # insert new column with date
        df.insert(1, "time", df.index.time)  # insert new column with time
        return df


    """Creates the groupped dataframe and the list of unique dates in the dataframe"""


    def create_unique_dates(self, df):
        avg_day = df.groupby(
            ["date"]).sum()  # the main dataframe is grouped by the day feature and values are aggregated (sum)
        day_list = list(avg_day.index)  # list of unique dates
        return avg_day, day_list


    """ Functions for working with days """


    def n_previous_days(self, date, n):
        td = datetime.timedelta(days=1)  # substract the necessary amount of days
        for i in range(1, n + 1):  # n here is 3   range(1, 4)
            date = date - td  # 3 times we take one week ago day [date-1 week, date-2 weeks, date-3 weeks]
            yield date


    def is_weekend(self, day):
        if day.weekday() > 4:
            return True
        return False


    def get_days(self, target_day, n, weekend_flag):
        """Returns an array with 3 dates - 3 previous same days (for example, three previous mondays)"""
        days = []  # empty list for days
        for day in self.n_previous_days(target_day, n):
            ####this part does nothing
            if (weekend_flag == 0):
                if self.is_weekend(day):
                    continue

            if (weekend_flag == 1):
                if not (self.is_weekend(day)):
                    continue
            days.append(day)  # for loop returns a list of 3 values
        return days


    def get_NSP_days(self, condition, n_days):  # 6th 13th and 20th day back
        same_days = []  # empty list for the 6th 13th and 20th day back
        for i in range(n_days):  # range (0 1 2)
            same_days.append(
                condition[i * 7 + 6])  # i = 0: 6, i = 1: 13, i = 2: 20 it intends to take the 6th 13th and 20th day back?
            if len(same_days) >= n_days:  # if the same ammount as N_days parameter, return these [3] days
                return same_days
        return same_days


    def get_base_value(self, df, condition):
        fitting_days_values = df[df["date"].isin(condition)].copy()[["date", "time", "E_load_Wh"]]
        result = fitting_days_values.groupby(["time"]).mean()  # group by 15 minutes intervals -
        return result


