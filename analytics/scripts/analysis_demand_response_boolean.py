import pandas as pd
from analytics.analysis import Analysis
import datetime
from analytics import utils
import numpy as np

"""
Demand-response. Calculation of booleans for discharge hours
"""

CLASS_NAME = "DemandResponseBooleanAnalysis"
ANALYSIS_NAME = "demand-response-boolean"
A_ARGS = {"analysis_code": "DEMAND_RESPONSE_BOOLEAN",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Calculates the RRMSE of demand-response",
          "output": "1 time series (of boolean values)",
          "mode": "rw",
          "inputs_count": 1,
          "outputs_count": 1,
          "inputs_outputs_always_same_count": True,
          "parameters": [
              {"name": "target_day", "count": 1, "type": "DATE", "info": "target day for analysis"},
              {"name": "exception_days", "count": -1, "type": "DATE", "info": "days to exclude from analysis"},
              {"name": "except_weekends", "count": 1, "type": "BOOLEAN", "info": "except weekends from analysis"},
              {"name": "discharge_start_hour", "count": 1, "type": "INTEGER", "info": "discharge start hour"},
              {"name": "discharge_duration", "count": 1, "type": "INTEGER", "info": "discharge duration (hours)"},
              {"name": "discharge_value", "count": 1, "type": "FLOAT", "info": "discharge value"},
              {"name": "mode", "count": 1, "type": "SELECT", "options": ["fact", "expected"],
               "info": "comparison mode: fact - with real data, expected - with previous day"}
          ]}


class DemandResponseBooleanAnalysis(Analysis):

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

    def _analyze(self, p, b):
        try:
            super()._analyze(p, b)

            # additional parameters
            self.data['datetime'] = self.data.index
            self.data['date'] = self.data['datetime'].dt.date
            self.data['time'] = self.data['datetime'].dt.time

            # means for each day (среднесуточные)
            avg_day = self.data.groupby(['date']).mean()
            self.logger.debug("Mean per each day, for all given data:\n\n" + str(avg_day) + "\n")
            # measurements per day (to avoid non-complete data)
            measurements_per_day = self.data.groupby(['date']).count()
            self.logger.debug("Measurements for each day, for all data:\n\n" + str(measurements_per_day) + "\n")

            # 4th november was Sunday
            condition1 = self._get_fitting_workdays()
            self.logger.debug("Fitting days, condition 1: " + str(condition1))

            if len(condition1) == 0:
                raise Exception("Condition 1 has no data. Check the input data")

            last_10_days_mean = self._get_last_10_days_mean(condition1, avg_day)
            self.logger.debug("Mean for the last 10 days: " + str(last_10_days_mean))

            condition2 = self._get_10_fitting_days(last_10_days_mean, condition1, avg_day, measurements_per_day)
            self.logger.debug("Fitting days, condition 2: " + str(condition1))

            if len(condition2) < 10:
                self.logger.warning('Only data for ' + str(len(condition2)) + ' days exists')
            if len(condition2) == 0:
                raise Exception("Condition 2 has no data. Check the input data")

            # base values
            b = self._get_base_value(self.data, condition2)
            self.logger.debug("Base values:\n\n" + str(b) + "\n")

            # last day values
            c = self._get_c(self.data, condition2)
            self.logger.debug("Last day:\n\n" + str(c) + "\n")

            a = self._get_correction(b, c)
            self.logger.debug("Correction: " + str(a))

            # adjust (0.8*b < b_adj < 1.2*b)
            b_adj = self._adjust(a, b)

            self.logger.debug("Adjusted base values:\n\n" + str(b_adj) + "\n")

            # apply discharge
            b_discharged = self._discharge(b_adj)

            b_to_compare, c_date = self._get_day_to_compare_with_discharged(self.data, measurements_per_day, condition2)

            bool = self._booleans(b_discharged, b_to_compare, self.target_day)

            return bool
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
            self._check_target_day()
            self._check_exception_days()
            self._check_except_weekends()
            self._check_discharge_value()
            self._check_discharge_start_hour()
            self._check_discharge_duration()
            self._check_discharge()
            self._check_mode()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_target_day(self):
        """
        Checks 'target_day' parameter
        """
        try:
            self.target_day = utils.string_to_date(self.parameters['target_day'][0])

            self.logger.debug("Parsed parameter 'target_day': " + str(self.target_day))
        except Exception as err:
            self.logger.error("Wrong parameter 'target_day': " + str(self.target_day) + " " + str(err))
            raise Exception("Wrong parameter 'target_day': " + str(self.target_day) + " " + str(err))

    def _check_exception_days(self):
        """
        Checks 'exception_days' parameter
        """
        try:
            self.exception_days = self._strings_to_dates(self.parameters['exception_days'])
            self.logger.debug("Parsed parameter 'exception_days': " + str(self.exception_days))
        except Exception as err:
            self.exception_days = []
            self.logger.debug("Parameter 'exception_days': " + str(self.exception_days) + " is empty")

    def _check_except_weekends(self):
        """
        Checks 'except_weekends' parameter
        """
        try:
            self.except_weekends = self.parameters['except_weekends'][0] in ['True', 'true']
            self.logger.debug("Parsed parameter 'except_weekends': " + str(self.except_weekends))
        except Exception as err:
            self.logger.error("Wrong parameter 'except_weekends': " + str(self.except_weekends) + " " + str(err))
            raise Exception("Wrong parameter 'except_weekends': " + str(self.except_weekends) + " " + str(err))

    def _check_mode(self):
        """
        Selects mode from: 'fact' and 'expected'

        :return:
        """
        try:
            self.mode = self.parameters['mode'][0]
            if self.parameters['mode'][0] not in ['fact', 'expected']:
                raise Exception("mode must be either 'fact' or 'expected', given value: " + self.mode)
            self.logger.debug("Parsed parameter 'mode': " + str(self.mode))
        except Exception as err:
            self.logger.error("Wrong parameter 'mode': " + str(self.mode) + " " + str(err))
            raise Exception("Wrong parameter 'mode': " + str(self.mode) + " " + str(err))

    @staticmethod
    def _n_previous_days(date, n):
        """
        Generator with n days previous to given one

        :param date: given date
        :param n: number of previous days
        :return: n previous days (all of them)
        """
        td = datetime.timedelta(days=1)

        for i in range(1, n + 1):
            date = date - td
            yield date

    @staticmethod
    def _is_weekend(day):
        """
        Checks if date is a weekend

        :param day: date
        :return: flag
        """
        if day.weekday() > 4:
            return True
        return False

    def _is_exception(self, day):
        """
        Checks if date is contained in exceptions

        :param day: date
        :return: flag
        """

        if day in self.exception_days:
            return True
        return False

    @staticmethod
    def _strings_to_dates(strings):
        """
        Converts datestring to dates

        :param strings: array of datestings
        :return: array of dates
        """

        return [utils.string_to_date(s) for s in strings]

    # get fitting workdays, excludes weekends and exceptions
    # only_workdays - excludes weekends
    # exceptions - days to exclude (holidays, other days)
    # n - days to iterate through
    def _get_fitting_workdays(self, n=45):
        """
        Gets only fitting workdays (non-weekends and non-exceptions) from last n days

        :param n: amount of last days taken into considerations
        :return: working days, that fit conditions
        """

        days = []
        for day in self._n_previous_days(self.target_day, n):
            if self._is_weekend(day) and self.except_weekends:
                continue
            if self._is_exception(day):
                continue
            days.append(day)

        return days

    @staticmethod
    def _get_last_10_days_mean(condition1, avg_day):
        """
        Calculates mean for last 10 working days

        :param condition1: days that fit condition 1
        :param avg_day: averages matrix
        :return: mean of the last 10 days
        """
        last_10_days = []

        for day in condition1:
            if day in avg_day.index:
                last_10_days.append(day)
            if len(last_10_days) == 10:
                break

        return avg_day[avg_day.columns[0]][last_10_days].mean()

    @staticmethod
    def _get_10_fitting_days(last_10_days_mean, condition1, avg_day, mpd):
        """
        Get 10 (or less) days, fitting the condition 2, and these days have 24 measumenets

        :param last_10_days_mean: mean for the last 10 days
        :param condition1: days that fit condition 1
        :param avg_day: averages matrix
        :param mpd: measurements per day
        :return: fitting 10 days
        """
        fitting_days = []
        for day in condition1:
            if day in avg_day.index:
                if avg_day[avg_day.columns[0]][day] >= last_10_days_mean * 0.5 and mpd[mpd.columns[0]][day] == 24:
                    fitting_days.append(day)
            if len(fitting_days) >= 10:
                return fitting_days
        return fitting_days

    @staticmethod
    def _get_base_value(df, condition2):
        """
        Calculate base values

        :param df: original dataframe with values
        :param condition2: days that fit condition 2
        :return: base values
        """
        fitting_days_values = df[df['date'].isin(condition2)].copy()

        fitting_days_values['time'] = fitting_days_values['datetime'].dt.time

        aggr = fitting_days_values.groupby(['time']).mean()

        return aggr

    @staticmethod
    def _get_c(df, condition2):
        """
        Get cs

        :param df: original dataframe with values
        :param condition2: days that fit condition 2
        :return: correction values
        """
        # get only previous day values
        temp = pd.DataFrame()
        for i in range(len(condition2)):
            temp = df[df['date'].isin([condition2[i]])].copy()
            if len(temp) == 24:
                break
        if len(temp) != 24:
            raise Exception("All previous days have missing data, impossible to select one to calculate correction")
        # this is unnecessary, but is done to remove spare columns (make data look alike b)
        return temp.groupby(['time']).mean()

    @staticmethod
    def _get_correction(b, c):
        """
        Calculates correction (a)

        :param b: bases
        :param c: correction
        :return: corrected (not adjusted) values
        """
        a16 = c.iloc[16] - b.iloc[16]
        a17 = c.iloc[17] - b.iloc[17]
        return (a16 + a17) / 2

    @staticmethod
    def _adjust(a, b):
        """
        Adjust according to the thresholds

        :param a: correction
        :param b: base
        :return: adjusted values
        """
        b_adj = b + a

        b_high = b * 1.2
        b_low = b * 0.8

        flag = b_adj > b_high

        b_adj[flag] = b_high[flag]

        flag = b_adj < b_low

        b_adj[flag] = b_low[flag]

        return b_adj

    def _check_discharge_value(self):
        """
        Checks 'discharge_value' parameter
        """
        try:
            self.discharge_value = float(self.parameters['discharge_value'][0])
            self.logger.debug("Parsed parameter 'discharge_value': " + str(self.discharge_value))
        except Exception as err:
            self.logger.error("Wrong parameter 'discharge_value': " + str(self.discharge_value) + " " + str(err))
            raise Exception("Wrong parameter 'discharge_value': " + str(self.discharge_value) + " " + str(err))

    def _check_discharge_duration(self):
        """
        Checks 'discharge_duration' parameter
        """
        try:
            self.discharge_duration = int(self.parameters['discharge_duration'][0])
            self.logger.debug("Parsed parameter 'discharge_duration': " + str(self.discharge_duration))
        except Exception as err:
            self.logger.error("Wrong parameter 'discharge_duration': " + str(self.discharge_duration) + " " + str(err))
            raise Exception("Wrong parameter 'discharge_duration': " + str(self.discharge_duration) + " " + str(err))

    def _check_discharge_start_hour(self):
        """
        Checks 'discharge_start_hour' parameter
        """
        try:
            self.discharge_start_hour = int(self.parameters['discharge_start_hour'][0])
            self.logger.debug("Parsed parameter 'discharge_start_hour': " + str(self.discharge_start_hour))
        except Exception as err:
            self.logger.error(
                "Wrong parameter 'discharge_start_hour': " + str(self.discharge_start_hour) + " " + str(err))
            raise Exception(
                "Wrong parameter 'discharge_start_hour': " + str(self.discharge_start_hour) + " " + str(err))

    def _check_discharge(self):
        """
        Chechs if discharge is possible
        :return:
        """
        try:
            if self.discharge_start_hour > 23 or self.discharge_start_hour < 0:
                raise Exception("Discharge start range [0, 23], discharge start: " + str(self.discharge_start_hour))
            discharge_final_hour = self.discharge_start_hour + self.discharge_duration
            if discharge_final_hour > 24:
                raise Exception("Discharge end range [1, 24], discharge end: " + str(discharge_final_hour))
            self.logger.debug(
                "Discharge possible, from " + str(self.discharge_start_hour) + " to " + str(discharge_final_hour))
        except Exception as err:
            self.logger.error("Discharge impossible: " + str(err))
            raise Exception("Discharge impossible: " + str(err))

    def _discharge(self, b_adj):
        """
        Discharges given hours

        :param b_adj: baseline to discharge
        :return:
        """
        b_dc = b_adj.copy()
        try:
            for i in range(self.discharge_duration):
                b_dc.iloc[self.discharge_start_hour + i] -= self.discharge_value
        except Exception as err:
            self.logger.error("Impossible to discharge: " + str(err))

        return b_dc

    def _get_day_to_compare_with_discharged(self, df, mpd, cond2):
        """
        Returns the day to compare with discharged line

        :param df: data
        :param mpd: measurements per day
        :param cond2:
        :return:
        """
        # fact data (with existance checks)
        if self.mode == 'fact':
            if self.target_day in mpd.index and mpd[mpd.columns[0]][self.target_day] == 24:
                target_day = df[df['date'].isin([self.target_day])].copy()
                day_data = target_day.groupby(['time']).mean()
                date = self.target_day
            else:
                raise Exception("'fact'-mode selected, but target_day data is not full or not provided")
        # expected data
        else:
            target_day = df[df['date'].isin([cond2[0]])].copy()
            working_last = target_day.groupby(['time']).mean()
            day_data = self._discharge(working_last)
            date = cond2[0]
        return day_data, date

    def _booleans(self, b_d, b_c, c_date):
        """
        Calculates booleans

        :param b_d: discharged data
        :param b_c: discharged data to compare (fact or discharged last fitting day)
        :param c_date: comparison date
        :return:
        """
        try:
            df = pd.DataFrame()
            df['d'] = b_d[b_d.columns[0]]
            df['c'] = b_c[b_c.columns[0]]
            df = df[self.discharge_start_hour:self.discharge_start_hour + self.discharge_duration]
            df['bool'] = df['d'] >= df['c']
            df['int'] = df['bool'].astype(float)

            time_start = datetime.datetime.combine(c_date, df.index[0])

            # return as dataframe
            return pd.DataFrame(np.array(df['int']), pd.date_range(time_start, periods=len(df), freq='1H'), ['value'])
        except Exception as err:
            self.logger.error("Impossible calculate rrmse: " + str(err))

    def _prepare_for_output(self, p, d, res):
        """
        format results for output

        :param res: unformatted results
        :return: formatted results
        """

        return res
