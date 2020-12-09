import logging
import os
import padasip as pa
import pandas as pd
import numpy as np
from analytics.analysis import Analysis
"""
RMSE function.
"""

CLASS_NAME = "analysisDemandResponseRMSE"
ANALYSIS_NAME = "analysis-demand-response-rmse"
A_ARGS = {"analysis_code": "ANALYSISDEMANDRESPONSERMSE",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "RMSE",
          "output": "1 time series (on target day)",
          "inputs_count": 1,
          "outputs_count": 1,
          "inputs_outputs_always_same_count": True,
          "mode": "rw",
          "parameters": [

          ]}


class analysisDemandResponseRMSE(Analysis):
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
            data.columns = ['value1', 'value2']
            d1 = self._preprocess_df(data[['value1']])
            d2 = self._preprocess_df(data[['value2']])
            res = self._analyze(d1, d2)
            res = res.astype(float)
            # print(res)
            return res
        except Exception as err:
            self.logger.error(err)
            raise Exception(str(err))


    def _analyze(self, d1, d2):
        """
        Analyze: Main body of analysis

        :param p: parsed analysis parameters
        :param d: preprocessed analysis data

        :return: df with results
        """
        self.logger.debug("Start analyze")
        try:
            date_list = self.create_unique_dates(self.separate_dt(d1))
            return self.run_RMSE(d1, self.separate_dt(d2), date_list[1])

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
        data.columns = ['date_time', 'value']
        format_out = '%Y-%m-%d %H:%M:%S'
        # format_in = '%Y-%m-%d %H:%M:%S+00:00'
        data['date_time'] = data['date_time'].apply(lambda x: x.strftime(format_out))
        data['date_time'] = pd.to_datetime(data['date_time'])
        data.set_index("date_time", inplace=True)
        return data

    def run_RMSE(self, df1, df2, day_list):
        """ Forecasting LOAD by Coping previous day depending on day position in a week
        It not just copy the previous value 3 weeks ago, it finds average among three previous same days
        For example, average of the 3 previous mondays"""
        self.logger.debug("START RMSE")
        try:
            if len(df1) != len(df2):
                self.logger.error("Different input data lengths")
                raise Exception("Different input data lengths")
            df1["RMSE_calc"] = 0  # the column with the error in the df is initialized with 0s
            for i in range(len(day_list)):
                copy1 = df1[df1["date"].isin([list(day_list)[i]])].copy()[["date", "time", "value"]]
                copy2 = df2[df2["date"].isin([list(day_list)[i]])].copy()[["date", "time", "value"]]
                x1 = np.zeros((len(copy1)))
                x2 = np.zeros((len(copy2)))

                for j in range(len(copy1)):
                    x1[j] = copy1.loc[copy1.index[j], "value"]
                    x2[j] = copy2.loc[copy2.index[j], "value"]

                rmse = pa.misc.RMSE(x1, x2)
                for j in range(len(copy1)):
                    df1.loc[copy1.index[j], "RMSE_calc"] = rmse
            return df1[["RMSE_calc"]]

        except Exception as err:
            self.logger.error("Error in run_RMSE: " + str(err))
            raise Exception("Error in run_RMSE: " + str(err))

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


