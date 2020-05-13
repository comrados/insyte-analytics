import pandas as pd
from analytics.analysis import Analysis
import datetime
from analytics import utils
import numpy as np

"""
Demand-response. Calculation of RRMSE between baseline and prediction/fact.
"""

CLASS_NAME = "DemandResponseBaselineApplicabilityAnalysis"
ANALYSIS_NAME = "demand-response-baseline-applicability"
A_ARGS = {"analysis_code": "DEMAND_RESPONSE_BASELINE_APPLICABILITY",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Checks the applicability of demand-response's baseline",
          "output": "1 time series (6 values)",
          "mode": "rw",
          "parameters": [
              {"name": "target_day", "count": 1, "type": "DATE", "info": "target day for analysis"},
              {"name": "exception_days", "count": -1, "type": "DATE", "info": "days to exclude from analysis"},
              {"name": "except_weekends", "count": 1, "type": "BOOLEAN", "info": "except weekends from analysis"},
              {"name": "peak_hours", "count": -1, "type": "INTEGER", "info": "array of planned peak hours"},
              {"name": "adjustment_hours", "count": -1, "type": "INTEGER", "info": "array of adjustment hours"}
          ]}


class DemandResponseBaselineApplicabilityAnalysis(Analysis):

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

            # additional parameters
            self.data['datetime'] = self.data.index
            self.data['date'] = self.data['datetime'].dt.date
            self.data['time'] = self.data['datetime'].dt.time

            # measurements per day (to avoid incomplete data)
            measurements_per_day = pd.DataFrame(self.data.groupby(['date'])['date'].count())

            # drop days with less than 24 records
            self._drop_incomplete_days(measurements_per_day)

            # workdays (no holidays (if given) and weekends (if true))
            working_days = self._get_fitting_workdays()
            if len(working_days) < 20:
                raise Exception("Not enough working days data (less than 20): " + str(len(working_days)))

            # days, except for first 10
            days_to_analyze = working_days[10:]

            # days to analyse and preceding 10 days
            days_for_baselines = {d: working_days[i:10 + i] for i, d in enumerate(days_to_analyze)}

            baselines = self._calculate_baselines(days_for_baselines)

            # means for each day (среднесуточные)
            avg_day = self.data.groupby(['date']).mean()

            last_10_days_mean = self._get_last_10_days_mean(working_days, avg_day)
            self.logger.debug("Mean for the last 10 days: " + str(last_10_days_mean))

            condition2 = self._get_10_fitting_days(last_10_days_mean, working_days, avg_day, measurements_per_day)
            self.logger.debug("Fitting days, condition 2: " + str(working_days))

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

            rrmse = self._rrmse(b_discharged, b_to_compare, c_date)

            return rrmse
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
            self._check_peak_hours()
            self._check_adjustment_hours()
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
            self.logger.error("Wrong parameter 'target_day': " + str(self.parameters['target_day']) + " " + str(err))
            raise Exception("Wrong parameter 'target_day': " + str(self.parameters['target_day']) + " " + str(err))

    def _check_exception_days(self):
        """
        Checks 'exception_days' parameter
        """
        try:
            self.exception_days = self._strings_to_dates(self.parameters['exception_days'])
            self.logger.debug("Parsed parameter 'exception_days': " + str(self.exception_days))
        except Exception:
            self.exception_days = []
            self.logger.debug("Wrong parameter 'exception_days': " + str(self.parameters['exception_days']) + " ")

    def _check_except_weekends(self):
        """
        Checks 'except_weekends' parameter
        """
        try:
            self.except_weekends = self.parameters['except_weekends'][0] in ['True', 'true', True]
            self.logger.debug("Parsed parameter 'except_weekends': " + str(self.except_weekends))
        except Exception as err:
            self.logger.error(
                "Wrong parameter 'except_weekends': " + str(self.parameters['except_weekends']) + " " + str(err))
            raise Exception(
                "Wrong parameter 'except_weekends': " + str(self.parameters['except_weekends']) + " " + str(err))

    def _check_peak_hours(self):
        try:
            self.peak_hours = [int(i) for i in self.parameters['peak_hours']]
            self.logger.debug("Parsed parameter 'peak_hours': " + str(self.peak_hours))
        except Exception as err:
            self.logger.error("Wrong parameter 'peak_hours': " + str(self.parameters['peak_hours']) + " " + str(err))
            raise Exception("Wrong parameter 'peak_hours': " + str(self.parameters['peak_hours']) + " " + str(err))

    def _check_adjustment_hours(self):
        try:
            self.adjustment_hours = [int(i) for i in self.parameters['adjustment_hours']]
            self.logger.debug("Parsed parameter 'adjustment_hours': " + str(self.adjustment_hours))
        except Exception as err:
            self.logger.error(
                "Wrong parameter 'adjustment_hours': " + str(self.parameters['adjustment_hours']) + " " + str(err))
            raise Exception(
                "Wrong parameter 'adjustment_hours': " + str(self.parameters['adjustment_hours']) + " " + str(err))

    def _drop_incomplete_days(self, mpd):
        dropped_days = {}
        for idx, row in mpd.iterrows():
            if row[0] < 24:
                self.data.drop(self.data[self.data["date"] == idx].index, inplace=True)
                dropped_days[idx] = row[0]
        self.logger.warning("These days were excluded due to incomplete data: " + str(dropped_days))

    @staticmethod
    def _dates_generator(min_d, max_d):
        """
        Generator with n days previous to given one

        :param min_d: min date
        :param max_d: max date
        :return: dates inbetween
        """
        td = datetime.timedelta(days=1)

        date = min_d

        while date <= max_d:
            yield date
            date = date + td

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
    def _get_fitting_workdays(self):
        """
        Gets only fitting workdays (non-weekends and non-exceptions) from last n days

        :param n: amount of last days taken into considerations
        :return: working days, that fit conditions
        """

        days = []
        for day in self._dates_generator(self.data['date'].min(), self.data['date'].max()):
            if self._is_weekend(day) and self.except_weekends:
                continue
            if self._is_exception(day):
                continue
            days.append(day)

        return days

    def _calculate_baselines(self, days_for_baselines):
        baselines = {}
        for date, days_avg in days_for_baselines.items():
            data_for_date = self.data.loc[self.data['date'].isin(days_avg)]
            baseline_for_date = data_for_date.groupby(['time']).mean()
            baselines[date] = baseline_for_date
        return baselines

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

    def _prepare_for_output(self, p, d, res):
        """
        format results for output

        :param res: unformatted results
        :return: formatted results
        """

        return res
