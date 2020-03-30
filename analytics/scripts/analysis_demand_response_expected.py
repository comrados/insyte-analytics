import pandas as pd
from analytics.analysis import Analysis
import datetime
from analytics import utils
import numpy as np

"""
Demand-response. Expected data (if actual 'fact'-line is unknown): last working day (+ discharge), fitting condition 2
"""

CLASS_NAME = "DemandResponseExpectedAnalysis"
ANALYSIS_NAME = "demand-response-expected"
A_ARGS = {"analysis_code": "DEMAND_RESPONSE_EXPECTED",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Calculates the expected line of demand-response",
          "output": "1 time series",
          "parameters": [
              {"name": "target_day", "count": 1, "type": "DATE", "info": "target day for analysis"},
              {"name": "exception_days", "count": -1, "type": "DATE", "info": "days to exclude from analysis"},
              {"name": "except_weekends", "count": 1, "type": "BOOLEAN", "info": "except weekends from analysis"},
              {"name": "discharge_start_hour", "count": 1, "type": "INTEGER", "info": "discharge start hour"},
              {"name": "discharge_duration", "count": 1, "type": "INTEGER", "info": "discharge duration (hours)"},
              {"name": "discharge_value", "count": 1, "type": "FLOAT", "info": "discharge value"}
          ]}


class DemandResponseExpectedAnalysis(Analysis):

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")

    def analyze(self):
        try:
            super().analyze()

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

            b_expected = self._get_expected_data(self.data, condition2)

            return b_expected
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
            self._check_target_day()
            self._check_exception_days()
            self._check_except_weekends()
            self._check_discharge_value()
            self._check_discharge_start_hour()
            self._check_discharge_duration()
            self._check_discharge()
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

    def _n_previous_days(self, date, n):
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

    def _is_weekend(self, day):
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

    def _strings_to_dates(self, strings):
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

    def _get_last_10_days_mean(self, condition1, avg_day):
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

    def _get_10_fitting_days(self, last_10_days_mean, condition1, avg_day, mpd):
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

    def _get_expected_data(self, df, cond2):
        """
        Returns the day to compare with discharged line

        :param df: data
        :param cond2:
        :return:
        """

        target_day = df[df['date'].isin([cond2[0]])].copy()
        working_last = target_day.groupby(['time']).mean()
        day_data = self._discharge(working_last)
        return pd.DataFrame(np.array(day_data), pd.date_range(self.target_day, periods=24, freq='1H'), ['expected'])
