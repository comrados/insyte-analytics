import logging
import os
import datetime
import pandas as pd
import numpy as np
from analytics.analysis import Analysis
from padasip.filters.base_filter import AdaptiveFilter

"""
Generate PAR forecast function.
"""

CLASS_NAME = "analysisPredictionPar"
ANALYSIS_NAME = "analysis-prediction-par"
A_ARGS = {"analysis_code": "ANALYSISPREDICTIONPAR",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Generate forecast with the use PAR method",
          "output": "1 time series (on target day)",
          "inputs_count": 1,
          "outputs_count": 1,
          "inputs_outputs_always_same_count": True,
          "mode": "rw",
          "parameters": [
              {"name": "target_day", "count": 1, "type": "DATE", "info": "target day for analysis"},
          ]}

class FilterRLS(AdaptiveFilter):
    """
    Adaptive RLS filter.
    **Args:**
    * `n` : length of filter (integer) - how many input is input array (row of input matrix)
    **Kwargs:**
    * `mu` : forgetting factor (float). It is introduced to give exponentially
             less weight to older error samples. It is usually chosen
             between 0.98 and 1.
             * `eps` : initialisation value (float). It is usually chosen
             between 0.1 and 1.
             * `w` : initial weights of filter. Possible values are:
                 * array with initial weights (1 dimensional array) of filter size
                 * "random" : create random weights
                 * "zeros" : create zero value weights
    """

    def __init__(self, n, mu=0.99, eps=0.1, w="random"):
        self.kind = "RLS filter"
        if type(n) == int:
            self.n = n
        else:
            raise ValueError('The size of filter must be an integer')
        self.mu = self.check_float_param(mu, 0, 1, "mu")
        self.eps = self.check_float_param(eps, 0, 1, "eps")
        self.init_weights(w, self.n)
        self.R = 1/self.eps * np.identity(n)
        self.w_history = False

    def adapt(self, d, x):
        """
        Adapt weights according one desired value and its input.
        **Args:**
        * `d` : desired value (float)
        * `x` : input array (1-dimensional array)
        """
        y = np.dot(self.w, x)
        e = d - y
        R1 = np.dot(np.dot(np.dot(self.R,x),x.T),self.R)
        R2 = self.mu + np.dot(np.dot(x,self.R),x.T)
        self.R = 1/self.mu * (self.R - R1/R2)
        dw = np.dot(self.R, x.T) * e
        self.w += dw
        return y, e, self.w

    def run(self, d, x):
        """
        This function filters multiple samples in a row.
        **Args:**
        * `d` : desired value (1 dimensional array)
        * `x` : input matrix (2-dimensional array). Rows are samples, columns are input arrays.
        **Returns:**
        * `y` : output value (1 dimensional array).
          The size corresponds with the desired value.
        * `e` : filter error for every sample (1 dimensional array).
          The size corresponds with the desired value.
        * `w` : history of all weights (2 dimensional array).
          Every row is set of the weights for given sample.
        """
        # measure the data and check if the dimmension agree
        N = len(x)
        if not len(d) == N:
            raise ValueError('The length of vector d and matrix x must agree.')
        self.n = len(x[0])
        # prepare data
        try:
            x = np.array(x)
            d = np.array(d)
        except:
            raise ValueError('Impossible to convert x or d to a numpy array')
        # create empty arrays
        y = np.zeros(N)
        e = np.zeros(N)
        self.w_history = np.zeros((N, self.n))
        # adaptation loop
        for k in range(N):
            self.w_history[k,:] = self.w
            y[k] = np.dot(self.w, x[k])
            e[k] = d[k] - y[k]
            R1 = np.dot(np.dot(np.dot(self.R,x[k]),x[k].T),self.R)
            R2 = self.mu + np.dot(np.dot(x[k],self.R),x[k].T)
            self.R = 1/self.mu * (self.R - R1/R2)
            dw = np.dot(self.R, x[k].T) * e[k]
            self.w += dw
        return y, e, self.w_history

class analysisPredictionPar(Analysis):
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
            res = res.set_index('date_time')
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
            return self.run_PAR(p, d, date_list[1])

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
        data['date_time'] = data['date_time'].apply(lambda x: x.strftime(format_out))
        # print(datetime.timedelta(days=1))
        data['date_time'] = pd.to_datetime(data['date_time'])
        # print((data['date_time'].iloc[-1].date()))
        add_time = data['date_time'].iloc[-1]
        list_of_hours = []
        for i in range(24):
            add_time = add_time + datetime.timedelta(hours=1)
            list_of_hours.append(add_time)
        # print(list_of_hours)
        # data = data.append(pd.DataFrame(list_of_hours, columns=['date_time']),ignore_index=True)

        data.set_index("date_time", inplace=True)
        data.fillna(0, inplace=True)
        return data

    def run_PAR(self, p, df, day_list):
        """ Forecasting LOAD by Coping previous day depending on day position in a week
        It not just copy the previous value 3 weeks ago, it finds average among three previous same days
        For example, average of the 3 previous mondays"""
        self.logger.debug("START PAR")
        try:
            df['val_par'] = 0  # initializes the E_load_forecast_PAR_Wh column with zeros
            target_day = pd.to_datetime(p['target_day'], errors='ignore')
            max_target_day = day_list[len(day_list) - 1] + datetime.timedelta(days=1)
            if (target_day > max_target_day):
                self.logger.error("Target day outside of analysis")
                return False;
            # RLS parameters
            num_par = 4  # number of alfa for AR model, a1,a2,a3,a4
            num_m = 3  # number of circles of the data calculations
            # print(f'inxd {day_list.index(target_day)}')
            if (target_day == max_target_day):
                num_s = day_list.index(day_list[len(day_list) - 1])+2
                num_s_max = day_list.index(day_list[len(day_list) - 1])+1
            else:
                i = 1
                for dt in day_list:
                    if str(dt) == p['target_day']:
                        break
                    i = i + 1
                num_s = i
                num_s_max = i
            N = len(df.loc[df.index.date == day_list[len(day_list) - 1]])
            time_interval = 24/N * 60 #minutes
            day = 0  # day of the number of days used so far
            n_rls = 3

            y_estimate = np.zeros((N, num_m * num_s))
            average = np.zeros((N, num_m * num_s))
            LOAD_data = np.zeros((N, num_m * num_s))
            t = 0
            w = np.zeros((N, num_par))
            w_list = np.zeros((num_m * num_s * N, num_par))  # 1566*4

            print("1st out of 3 loops")
            for i in range(num_m * num_s):
                # if i is in range (0,1566) and num_s = 522
                # then [i%num_s] is one number out of a list [0 1 2 3 ... 521 0 1 2 3 ... 521 0 1 2 3 ... 521 0]
                # so we take one date out of 522 consistently three times

                # conditio n df["date"].isin([list(day_list)[i%num_s]])
                # returns the list of True or False values with the len of the dataframe, and True filter helps to take values of the chosen day (the day index is [i%num_s])
                # df[condition].copy()[["E_load_Wh"]] returns series of loads of one day (96 instances)
                if (((i+1) % (num_s)) != 0 or i == 0):
                    copy_from_data = df[df["date"].isin([list(day_list)[i % num_s]])].copy()[["E_load_Wh"]]
                    for j in range(len(copy_from_data)):  # for each instance of one day  cause j is in (0; 95)
                        l = copy_from_data.index[j]
                        # l is the index of the instance (%d.%m.%Y %H:%M:%S)
                        LOAD_data[j, i] = copy_from_data.loc[l, "E_load_Wh"]  # LOAD_data.shape(96, 1566)
                else:
                    for j in range(N):
                        LOAD_data[j, i] = 0
                    # 1566 columns in LOAD_data means each day is taken three times
                    # 96 rows in LOAD_data means all instances of the day are taken
                    # LOAD_data[:,0] and LOAD_data[:,522]) are identical
            filt = FilterRLS(4, mu=0.999, eps=1e-8)  # method of weights optimization

            print("2nd out of 3 loops")
            for m in range(num_m):  # multiple passes through the same data
                for s in range(0, num_s):  # for each available day in the data
                    y_before = np.zeros((N, num_par))

                    for t in range(N):  # for each 15 mins during the day
                        diff_n = t - n_rls
                        average[t, day] = 0  # Computing average of last three similar days of the same interval
                        if (day % num_s >= 28):
                            average[t, day] = (LOAD_data[t, day - 7] + LOAD_data[t, day - 14] + LOAD_data[t, day - 21] +
                                               LOAD_data[t, day - 28]) / 4
                        else:
                            if ((day > 0) & (day % num_s < 28)):
                                average[t, day] = LOAD_data[t, day - 1]

                        if (diff_n == -3):  # Creating the vector of previous estimates
                            y_before[t, :] = [LOAD_data[N - 1, day - 1], LOAD_data[N - 2, day - 1],
                                              LOAD_data[N - 3, day - 1], average[t, day]]
                        else:
                            if (diff_n == -2):
                                y_before[t, :] = [y_estimate[0, day], LOAD_data[N - 1, day - 1],
                                                  LOAD_data[N - 2, day - 1], average[t, day]]
                            else:
                                if (diff_n == -1):
                                    y_before[t, :] = [y_estimate[1, day], y_estimate[0, day], LOAD_data[N - 1, day - 1],
                                                      average[t, day]]
                                else:
                                    y_before[t, :] = [y_estimate[t - 1, day], y_estimate[t - 2, day],
                                                      y_estimate[t - 3, day], average[t, day]]

                        y_estimate[t, day] = np.dot(w[t], y_before[t, :].T)

                        # if (y_estimate[t, day] > 2000):
                        #     y_estimate[t, day] = 2000
                        # else:
                        if (y_estimate[t, day] < 0):
                            y_estimate[t, day] = 0

                        w_list[N * day + t] = w[t]
                    # print(f'LOAD_data[:, day]: {LOAD_data[:, day]}')
                    # print(f'w: {w}')
                    if (day+1) % (num_s):
                        try:
                            y, e, w = filt.run(LOAD_data[:, day], y_before)
                        except:
                            pass
                    day = day + 1  # day keeps track of the total number of used day
            print(f'day {day}')
            print(f'y_estimate {y_estimate}')
            print("3rd out of 3 loops")
            add_time = target_day
            list_of_time = []
            list_of_estimate = []
            day = num_m*num_s-1
            try:
                for t in range(N):
                    list_of_time.append(add_time)
                    list_of_estimate.append(y_estimate[t, day])
                    add_time = add_time + datetime.timedelta(minutes=time_interval)

                res = pd.DataFrame({'date_time': list_of_time, 'val_par': list_of_estimate})
                return res
            except Exception as err:
                self.logger.error("Error in run_PAR (list): " + str(err))
                raise Exception("Error in run_PAR (list): " + str(err))
            print(list_of_estimate)
            for i in range(len(day_list)):
                copy_to_data = df[df["date"].isin([list(day_list)[i]])].copy()[
                    ["date", "time", "E_load_Wh"]]
                for j in range(len(copy_to_data)):
                    l = copy_to_data.index[j]
                    df.loc[l, 'val_par'] = float(y_estimate[j, i + (num_m - 1) * num_s])

            # # rmse.RMSE_CALC(df, day_list, 'val_par', params.LFPARRMSE)
            fig = px.bar(df, x=df.index, y='val_par',

                         labels={'pop': 'population of Canada'}, height=400)
            # # Here we modify the tickangle of the xaxis, resulting in rotated labels.
            fig.update_layout(barmode='group', xaxis_tickangle=-45)
            # fig.show()
            print("END  PAR")
            print(df)
            # print(df.loc[df['val_par'] != 2000])
            return (w_list)
            # return copy_new_data

        except Exception as err:
            self.logger.error("Error in run_PAR: " + str(err))
            raise Exception("Error in run_PAR: " + str(err))

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


