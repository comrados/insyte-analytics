import logging
import pandas as pd
from analytics.analysis import Analysis
import datetime
from . import utils


class DemandResponseAnalysis(Analysis):
    logger = logging.getLogger('insyte_analytics.analytics.analysis_demand_response')

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")

    def analyze(self):
        super().analyze()
        try:
            self._parse_parameters()

            # additional parameters
            self.data['datetime'] = self.data.index
            self.data['date'] = self.data['datetime'].dt.date
            self.data['time'] = self.data['datetime'].dt.time

            # means for each day (среднесуточные)
            avg_day = self.data.groupby(['date']).mean()
            self.logger.debug("Mean per each day, for all given data:\n\n" + str(avg_day) + "\n")

            # 4th november was Sunday
            condition1 = self._get_fitting_workdays()
            self.logger.debug("Fitting days, condition 1: " + str(condition1))

            if len(condition1) == 0:
                raise Exception("Condition 1 has no data. Check the input data")

            last_10_days_mean = self._get_last_10_days_mean(condition1, avg_day)
            self.logger.debug("Mean for the last 10 days: " + str(last_10_days_mean))

            condition2 = self._get_10_fitting_days(last_10_days_mean, condition1, avg_day)
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

            b_adj.set_index(pd.date_range(self.target_day, periods=24, freq='1H'), inplace=True)
            b_adj.rename(columns={b_adj.columns[0]: 'value'}, inplace=True)
            self.logger.debug("Adjusted base values:\n\n" + str(b_adj) + "\n")

            return b_adj
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))

    def _parse_parameters(self):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters")
        try:
            self._check_target_day()
            self._check_exception_days()
            self._check_except_weekends()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_target_day(self):
        """
        Checks 'target_day' parameter
        """
        try:
            self.target_day = utils.string_to_date(self.parameters['target_day'][0]).date()

            self.logger.debug("Parsed parameter 'target_day': " + str(self.target_day))
        except Exception as err:
            self.logger.debug("Wrong parameter 'target_day': " + str(self.target_day) + " " + str(err))
            raise Exception("Wrong parameter 'target_day': " + str(self.target_day) + " " + str(err))

    def _check_exception_days(self):
        """
        Checks 'exception_days' parameter
        """
        try:
            self.exception_days = self._strings_to_dates(self.parameters['exception_days'])
            self.logger.debug("Parsed parameter 'exception_days': " + str(self.exception_days))
        except Exception as err:
            self.logger.debug("Wrong parameter 'exception_days': " + str(self.exception_days) + " " + str(err))
            raise Exception("Wrong parameter 'exception_days': " + str(self.exception_days) + " " + str(err))

    def _check_except_weekends(self):
        """
        Checks 'except_weekends' parameter
        """
        try:
            self.except_weekends = self.parameters['except_weekends'][0] in ['True', 'true']
            self.logger.debug("Parsed parameter 'except_weekends': " + str(self.except_weekends))
        except Exception as err:
            self.logger.debug("Wrong parameter 'except_weekends': " + str(self.except_weekends) + " " + str(err))
            raise Exception("Wrong parameter 'except_weekends': " + str(self.except_weekends) + " " + str(err))

    def _n_previous_days(self, date, n):
        td = datetime.timedelta(days=1)

        for i in range(1, n + 1):
            date = date - td
            yield date

    def _is_weekend(self, day):
        if day.weekday() > 4:
            return True
        return False

    def _is_exception(self, day):
        if day in self.exception_days:
            return True
        return False

    def _strings_to_dates(self, strings):
        return [utils.string_to_date(s).date() for s in strings]

    # get fitting workdays, excludes weekends and exceptions
    # only_workdays - excludes weekends
    # exceptions - days to exclude (holidays, other days)
    # n - days to iterate through
    def _get_fitting_workdays(self, n=45):
        days = []
        for day in self._n_previous_days(self.target_day, n):
            if self._is_weekend(day) and self.except_weekends:
                continue
            if self._is_exception(day):
                continue
            days.append(day)
        return days

    # calculates mean for last 10 working days
    def _get_last_10_days_mean(self, condition1, avg_day):
        last_10_days = []

        for day in condition1:
            if day in avg_day.index:
                last_10_days.append(day)
            if len(last_10_days) == 10:
                break

        return avg_day[avg_day.columns[0]][last_10_days].mean()

    # get 10 (or less) days, fitting the condition 2
    def _get_10_fitting_days(self, last_10_days_mean, condition1, avg_day):
        fitting_days = []
        for day in condition1:
            if day in avg_day.index:
                if avg_day[avg_day.columns[0]][day] >= last_10_days_mean * 0.5:
                    fitting_days.append(day)
            if len(fitting_days) >= 10:
                return fitting_days
        return fitting_days

    def _get_base_value(self, df, condition2):
        fitting_days_values = df[df['date'].isin(condition2)].copy()

        fitting_days_values['time'] = fitting_days_values['datetime'].dt.time

        aggr = fitting_days_values.groupby(['time']).mean()

        return aggr

    def _get_c(self, df, condition2):
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

    def _get_correction(self, b, c):
        a16 = c.iloc[16] - b.iloc[16]
        a17 = c.iloc[17] - b.iloc[17]
        return (a16 + a17) / 2

    def _adjust(self, a, b):
        b_adj = b + a

        b_high = b * 1.2
        b_low = b * 0.8

        flag = b_adj > b_high

        b_adj[flag] = b_high[flag]

        flag = b_adj < b_low

        b_adj[flag] = b_low[flag]

        return b_adj
