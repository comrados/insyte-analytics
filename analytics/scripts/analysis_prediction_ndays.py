import logging
import os
import datetime
import pandas as pd
import numpy as np
from analytics.analysis import Analysis

"""
Generate CLD forecast function.
"""

CLASS_NAME = "analysisPredictionNdays"
ANALYSIS_NAME = "analysis-prediction-ndays"
A_ARGS = {"analysis_code": "ANALYSISPREDICTIONNDAYS",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Generate forecast with the use copy last date method",
          "output": "1 time series (on target day)",
          "inputs_count": 1,
          "outputs_count": 1,
          "inputs_outputs_always_same_count": True,
          "mode": "rw",
          "parameters": [
              {"name": "target_day", "count": 1, "type": "DATE", "info": "target day for analysis"},
          ]}


class analysisPredictionNdays(Analysis):
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
            # print(d)
            res = self._analyze(p, d)
            res = res.astype(float)

            # res = self._prepare_for_output(p, d, res)

            # pd.options.display.max_columns = 100
            # print(res)
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
            return self.run_ND(p, d, date_list[1], date_list[0])

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

    def run_ND(self, p, df, day_list, avg_day):
        """ Forecasting LOAD by Coping previous day depending on day position in a week
        It not just copy the previous value 3 weeks ago, it finds average among three previous same days
        For example, average of the 3 previous mondays"""
        self.logger.debug("START N Days")
        try:
            df['val_nd'] = 0
            N_days = 10
            copy_from = N_days  # start from the N day in days list
            target_day = pd.to_datetime(p['target_day'], errors='ignore')
            max_target_day = day_list[len(day_list) - 1] + datetime.timedelta(days=1)
            if (target_day > max_target_day):
                self.logger.error("Target day outside of analysis")
                return []
            N = len(df.loc[df.index.date == day_list[len(day_list) - 1]])

            if not (self.is_weekend(target_day)):
                condition1 = self.get_days(target_day, N_days + 21, weekend_flag=0)  # 0 - no weekends

            if self.is_weekend(target_day):
                condition1 = self.get_days(target_day, N_days + 21,
                                                       weekend_flag=1)  # 1 - only weekends


            last_N_days_mean = self.get_last_N_days_mean(condition1, avg_day, "E_load_Wh", n_days=N_days)
            condition2 = self.get_N_days(last_N_days_mean, condition1, avg_day, "E_load_Wh", 0.5,
                                                     n_days=N_days)  # correction with 50% threshold of energy consumption
            b = self.get_base_value(df, condition2)
            c = self.get_last_day(df, condition2, N)
            a = self.get_correction(b, c)
            # print(f'b {b}')
            # print(f'c {c}')
            # print(f'a {a}')

            copy_new_data = self.adjust(a, b)  # adjust (0.8*b < b_adj < 1.2*b)
            copy_new_data.reset_index(inplace=True)
            copy_new_data['date_time'] = copy_new_data['time'].apply(lambda x: datetime.datetime.combine(target_day, x))
            copy_new_data.set_index("date_time", inplace=True)
            copy_new_data.drop(['time'], axis=1, inplace=True)
            copy_new_data.columns = ['val_nd']
            # print(f'copy_new_data {copy_new_data}')

            return copy_new_data
            # print(df[1899:1968])

        except Exception as err:
            self.logger.error("Error in run_ND: " + str(err))
            raise Exception("Error in run_ND: " + str(err))

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

    def get_last_N_days_mean(self, condition, avg_day, column, n_days):
        try:
            last_N_days = []
            for day in condition:
                if day.date() in avg_day.index:
                    last_N_days.append(day)
                    if len(last_N_days) == n_days:
                        break
            # print(f'last_N_days {last_N_days}')
            # print(avg_day[column][last_N_days])
            return avg_day[column][last_N_days].mean()
        except Exception as err:
            self.logger.error("Error in get_last_N_days_mean: " + str(err))
            raise Exception("Error in get_last_N_days_mean: " + str(err))

    def get_N_days(self, last_N_days_mean, condition, avg_day, column, value, n_days):
        try:
            some_days = []
            for day in condition:
                if day.date() in avg_day.index:
                    if avg_day[column][day.date()] >= last_N_days_mean * value:  # check the consumption, it should be more that 50% of mean from prev N days
                        some_days.append(day)
                if len(some_days) >= n_days:
                    return some_days
            return some_days

        except Exception as err:
            self.logger.error("Error in get_N_days: " + str(err))
            raise Exception("Error in get_N_days: " + str(err))

    def n_previous_days(self, date, n):
        td = datetime.timedelta(days=1)  # substract the necessary amount of days
        for i in range(1, n + 1):  # n here is 3   range(1, 4)
            date = date - td  # 3 times we take one week ago day [date-1 week, date-2 weeks, date-3 weeks]
            yield date

    def get_last_day(self, df, condition2, N):  # get only previous day values
        temp = pd.DataFrame()
        # print(f'condition2 {condition2}')
        for i in range(len(condition2)):
            temp = df[pd.to_datetime(df["date"]).isin([condition2[i]])].copy()[["date", "time", "E_load_Wh"]]
            if len(temp) == N:
                break
        return temp.groupby(['time']).mean()

    def get_correction(self, b, c):
        #    a_1 = c.iloc[n][EL] - b.iloc[n][EL]
        #    a_2 = c.iloc[n+1][EL] - b.iloc[n+1][EL]
        a_1 = np.mean(c["E_load_Wh"]) - np.mean(b["E_load_Wh"])
        a_2 = np.mean(c["E_load_Wh"]) - np.mean(b["E_load_Wh"])
        return (a_1 + a_2) / 2

    def adjust(self, a, b):
        b_adj = b + a
        b_high = b * 1.2
        b_low = b * 0.8
        flag = b_adj > b_high
        b_adj[flag] = b_high[flag]
        flag = b_adj < b_low
        b_adj[flag] = b_low[flag]
        return b_adj

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
        # print(f'df {df}')
        # print(f'condition {condition}')
        fitting_days_values = df[pd.to_datetime(df["date"]).isin(condition)].copy()[["date", "time", "E_load_Wh"]]
        result = fitting_days_values.groupby(["time"]).mean()  # group by 15 minutes intervals -
        # print(f'result {result}')
        return result


    # rmse.RMSE_CALC(df, day_list, "val_cld", "val_cld"RMSE)

# 2020-11-18 10:21:58.836 INFO analytics_server._content_to_json Thread-58 (140063009740544) JSON content: {'db_io_parameters': {'mode': 'rw', 'result_id': ['4966a9c1-6b8a-4a6a-ad86-c80337d5c2fc'], 'device_id': ['dd2af57e-fb5f-4852-a947-61524470b6f1'], 'data_source_id': ['166'], 'time_upload': ['2019-02-14_00:00:00+0000', '2019-04-01_23:59:59+0000'], 'limit': 'null'}, 'analysis_parameters': {'analysis': 'analysis-prediction-ndays', 'analysis_arguments': {'target_day': ['2019-03-31']}}}