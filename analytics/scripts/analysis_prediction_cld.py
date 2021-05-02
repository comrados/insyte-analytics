import logging
import os
import datetime
import pandas as pd
from analytics.analysis import Analysis

"""
Generate CLD forecast function.
"""

CLASS_NAME = "analysisPredictionCld"
ANALYSIS_NAME = "analysis-prediction-cld"
A_ARGS = {"analysis_code": "ANALYSISPREDICTIONCLD",
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


class analysisPredictionCld(Analysis):
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
            # print("Входные данные:")
            # print(data)
            d = self._preprocess_df(data)
            res = self._analyze(p, d)
            # print("Результат программы:")
            # print(res)
            res = res.astype(float)

            # res = self._prepare_for_output(p, d, res)

            # pd.options.display.max_columns = 100
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
            return self.run_CLD(p, d, date_list[1])

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
        data = self._preprocess_df_power(data)
        # data.set_index("date_time", inplace=True)
        return data

    def _preprocess_df_power(self, data):
        """
        Convert DataFrame to Power/hour
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs
            if data is not None:
                if data.empty:
                    raise Exception("Empty DataFrame")
                # print(pd.to_datetime(new_data.time).dt.minute)
                ndf = data.groupby(pd.Grouper(key="date_time", freq="1H")).count()
                ndf.columns = ['count_val']
                ndf['sum_val'] = data.groupby(pd.Grouper(key="date_time", freq="1H")).sum()
                ndf['sum_power'] = ndf.sum_val/ndf.count_val
                # ndf.reset_index(inplace=True)
                ndf.rename(columns={'sum_power': 'E_load_Wh'},
                                     inplace=True)
                return ndf[['E_load_Wh']]
            else:
                raise Exception("DataFrame is None")
            self.logger.debug("DataFrame preprocessed")
        except Exception as err:
            self.logger.error("Failed to preprocess DataFrame: " + str(err))
            raise Exception("Failed to preprocess DataFrame: " + str(err))

    def run_CLD(self, p, df, day_list):
        """ Forecasting LOAD by Coping previous day depending on day position in a week
        It not just copy the previous value 3 weeks ago, it finds average among three previous same days
        For example, average of the 3 previous mondays"""
        self.logger.debug("START Coping previous day")
        try:
            N_days = 3  # number of days to look back and copy
            # copy_from = N_days * 7  # start from the 21st day in days list
            format_out = '%Y-%m-%d'
            target_day = datetime.datetime.strptime(p['target_day'], format_out).date() # pd.to_datetime(p['target_day'], errors='ignore')
            max_target_day = day_list[len(day_list) - 1] + datetime.timedelta(days=1)
            if (target_day > max_target_day):
                self.logger.error("Target day outside of analysis")
                return []
            condition1 = self.get_days(target_day, N_days * 7,
                                       weekend_flag=2)  # days - array of 3 previous  same days, e.i. 3 previous Mondays
            condition2 = self.get_NSP_days(condition1, N_days)  # a list of 3 values
            # Average Data from the 3 same previous days with the same position
            copy_new_data = self.get_base_value(df, condition2)
            copy_new_data.reset_index(inplace=True)
            copy_new_data['date_time'] = copy_new_data['time'].apply(lambda x: datetime.datetime.combine(target_day, x))
            copy_new_data.set_index("date_time", inplace=True)
            copy_new_data.drop(['time'], axis=1, inplace=True)
            copy_new_data.columns = ['val_cld']
            return copy_new_data

        except Exception as err:
            self.logger.error("Error in run_CLD: " + str(err))
            raise Exception("Error in run_CLD: " + str(err))

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


    # rmse.RMSE_CALC(df, day_list, "val_cld", "val_cld"RMSE)
