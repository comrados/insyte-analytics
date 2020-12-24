import logging
import os
import datetime
import pandas as pd
import statsmodels.api as sm

from analytics.analysis import Analysis
"""
Generate SARIMA forecast function.
"""

CLASS_NAME = "analysisPredictionSarima"
ANALYSIS_NAME = "analysis-prediction-sarima"
A_ARGS = {"analysis_code": "ANALYSISPREDICTIONSARIMA",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Generate forecast with the use SARIMA method",
          "output": "1 time series (on target day)",
          "inputs_count": 1,
          "outputs_count": 1,
          "inputs_outputs_always_same_count": True,
          "mode": "rw",
          "parameters": [
              {"name": "target_day", "count": 1, "type": "DATE", "info": "target day for analysis"},
          ]}


class analysisPredictionSarima(Analysis):
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
            print(d)
            res = self._analyze(p, d)
            res = res.astype(float)

            # res = self._prepare_for_output(p, d, res)

            # pd.options.display.max_columns = 100
            print(res)
            return res
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
            return self.run_SARIMA(p, d, date_list[1])

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
        data.set_index("date_time", inplace=True)
        return data

    def run_SARIMA(self, p, df, day_list):
        """ Forecasting LOAD by Coping previous day depending on day position in a week
        It not just copy the previous value 3 weeks ago, it finds average among three previous same days
        For example, average of the 3 previous mondays"""
        self.logger.debug("START SARIMA")
        try:
            N = len(df.loc[df.index.date == day_list[len(day_list) - 2]])
            target_day = pd.to_datetime(p['target_day'], errors='ignore')
            max_target_day = day_list[len(day_list) - 1] + datetime.timedelta(days=1)
            if (target_day > max_target_day):
                self.logger.error("Target day outside of analysis")
                return False
            if (target_day < max_target_day):
                target_k = day_list.index(target_day)
            else:
                if (target_day == max_target_day):
                    target_k = len(day_list)

            if (len(day_list) >= 14):
                copy_from = 2 * 7   # start after this day
            if (len(day_list) >= 7 & len(day_list) < 14):
                copy_from = 7   # start after this day
            delta_target_day = len(day_list) - (max_target_day - target_day.date()).days
            if (len(day_list) < 7 or delta_target_day < 7):
                df['val_sarima'] = df['E_load_Wh']
                df.reset_index(inplace=True)
                df['date_time'] = df.date_time + datetime.timedelta(days=1)
                df.set_index('date_time', inplace=True)
                return df.loc[df.index.date == target_day][['val_sarima']]

            train = pd.DataFrame(df["E_load_Wh"][(target_k - copy_from) * N:(target_k) * N])
            train.index = pd.DatetimeIndex(train.index.values,
                                           freq=train.index.inferred_freq)
            p = 1  # первый лаг имеет значительную автокорелляцию по PACF
            d = 1  # дифференцировали один раз
            q = 1  # первый лаг имеет значительную автокорелляцию по ACF
            P = 1  # ACF положительно на 1 лаге
            D = 1  # наблюдается сезонность
            Q = 1  # ACF положительно на 1 лаге
            S = 48  # самое большое значение на ACF на 1 лаге

            model = sm.tsa.statespace.SARIMAX(train["E_load_Wh"], order=(p, d, q), seasonal_order=(P, D, Q, S)).fit(
                disp=-1)
            # print('Обучена')
            pred_data = model.predict(len(train)-N, len(train) + N - 1 + N, dynamic=True)
            pred_data = pd.DataFrame({'date_time': pred_data.index, 'val_sarima': pred_data.values})
            pred_data.set_index('date_time', inplace = True)
            return pred_data.loc[pred_data.index.date == target_day][['val_sarima']]
            # pred_data = pd.DataFrame(data=pred_data, columns=['val_sarima'])
            # print(pred_data)

        except Exception as err:
                self.logger.error("Error in run_SARIMA: " + str(err))
                raise Exception("Error in run_SARIMA: " + str(err))

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


