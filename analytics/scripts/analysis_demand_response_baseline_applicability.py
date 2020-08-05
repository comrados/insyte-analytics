import pandas as pd
from analytics.analysis import Analysis
import datetime
from analytics import utils

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
              {"name": "adjustment_hours", "count": -1, "type": "INTEGER", "info": "array of adjustment hours"},
              {"name": "method", "count": 1, "type": "SELECT", "options":["all_applicability","c_applicability",
                                                                          "rmse_none_applicability","rrmse_none_applicability",
                                                                          "rmse_all_applicability", "rrmse_all_applicability",
                                                                          "rmse_last_working_applicability", "rrmse_last_working_applicability",
                                                                          ""
                                                                          "", "rrmse_all_profile", "rrmse_none_profile",
                                                                          "rrmse_last_working_profile", "min_rrmse"], "info": "Method for calculating"},

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
            # out = self._prepare_for_output(None, None, res)
            # print(res)
            return res
        except Exception as err:
            self.logger.error(err)
            raise Exception(str(err))

    def _applicability(self, p, d):
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

            # table 2
            baselines = self._calculate_baselines(days_for_baselines)
            pd_bs = self._calculate_baselines_dates(days_for_baselines)
            original_lines = self._get_original_lines(days_to_analyze)
            modified_original_lines = self._get_only_adjustment_hours(original_lines)
            modified_baselines = self._get_only_adjustment_hours(baselines)

            # adjustment values
            adjustments_all = self._get_adjustments_all(baselines, original_lines)
            adjustments_last_working = self._adjustments_remove_last_working(adjustments_all)

            # adjust baselines (with 0.8 < adj < 1.2 restrictions)
            adjusted_baselines_all = self._adjust_baseline(modified_baselines, adjustments_all)
            adjusted_baselines_last_working = self._adjust_baseline(modified_baselines,
                                                                    adjustments_last_working)

            # calculate square errors
            error_none = modified_original_lines.sub(modified_baselines, fill_value=0.) ** 2
            error_all = modified_original_lines.sub(adjusted_baselines_all, fill_value=0.) ** 2
            error_last_working = modified_original_lines.sub(adjusted_baselines_last_working, fill_value=0.) ** 2

            # C, RMSE, RRMSE
            c = modified_original_lines.mean().mean()

            rmse_none = error_none.mean().mean() ** 0.5
            rmse_all = error_all.mean().mean() ** 0.5
            rmse_last_working = error_last_working.mean().mean() ** 0.5

            rrmse_none = rmse_none / c
            rrmse_all = rmse_all / c
            rrmse_last_working = rmse_last_working / c

            # return df
            # values order: c, rmse_none, rrmse_none, rmse_all, rrmse_all, rmse_last_working, rrmse_last_working
            res = [c, rmse_none, rrmse_none, rmse_all, rrmse_all, rmse_last_working, rrmse_last_working, pd_bs]

            return res
        except Exception as err:
            self.logger.error("Impossible to _applicability: " + str(err))
            raise Exception("Impossible to _applicability: " + str(err))

    def _analyze(self, p, d):
        """
        Run analysis.
        :return: output DataFrame
        """
        try:
            return self._dispatch_dict(self.parameters, d)
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))

    def _dispatch_dict(self, p, d):
        """
        Switch analysis.
        :return: output DataFrame
        """
        return {
            'all_applicability': lambda: self._prepare_for_output(None, None, self._applicability(None, None)),
            'c_applicability': lambda: self._prepare_for_output(None, None, [self._applicability(None, None)[0]]),
            'rmse_none_applicability': lambda: self._prepare_for_output(None, None, [self._applicability(None, None)[1]]),
            'rrmse_none_applicability': lambda: self._prepare_for_output(None, None, [self._applicability(None, None)[2]]),
            'rmse_all_applicability': lambda: self._prepare_for_output(None, None, [self._applicability(None, None)[3]]),
            'rrmse_all_applicability': lambda: self._prepare_for_output(None, None, [self._applicability(None, None)[4]]),
            'rmse_last_working_applicability': lambda: self._prepare_for_output(None, None, [self._applicability(None, None)[5]]),
            'rrmse_last_working_applicability': lambda: self._prepare_for_output(None, None, [self._applicability(None, None)[6]]),
            'baseline_none': lambda: self._applicability(None, None)[7],
            'min_rrmse': lambda: self._prepare_for_output(None, None, self._min_rrmse(self._applicability(None, None))),
            'min_profile': lambda: self._min_profile(None, None, self._applicability(None, None)),
            'rrmse_all_profile': lambda: self._rrmse_all_profile(None, None, self._applicability(None, None)),
            'rrmse_none_profile': lambda: self._rrmse_none_profile(None, None, self._applicability(None, None)),
            'rrmse_last_working_profile': lambda: self._rrmse_last_working_profile(None, None, self._applicability(None, None)),
        }.get(p['method'][0], lambda: self._prepare_for_output(None, None, self._applicability(None, None)))()

    def _min_profile(self, p, d, applicability):
        """
        Return profile with minimal rrmse.
        :return: output DataFrame
        """
        rrmse = [applicability[2],applicability[4], applicability[6]]

        min_rrmse = min(rrmse)
        new_d = self.data
        min_profile = new_d.iloc[:, [0]] * min_rrmse

        return min_profile

    def _min_rrmse(self, applicability):
        """
        Return profile with minimal rrmse.
        :return: output DataFrame
        """
        rrmse = [applicability[2],applicability[4], applicability[6]]

        min_rrmse = min(rrmse)
        if min_rrmse == applicability[2]:
            return ["rrmse_none"]
        if min_rrmse == applicability[4]:
            return ["rrmse_all"]
        if min_rrmse == applicability[6]:
            return ["rrmse_last_working"]

    def _rrmse_none_profile(self, p, d, applicability):
        """
        Return profile with rrmse_none.
        :return: output DataFrame
        """
        rrmse = [applicability[2],applicability[4], applicability[6]]

        select_rrmse = rrmse[0]
        new_d = self.data
        profile = new_d.iloc[:, [0]] * select_rrmse
        return profile

    def _rrmse_all_profile(self, p, d, applicability):
        """
        Return profile with rrmse_all.
        :return: output DataFrame
        """
        rrmse = [applicability[2],applicability[4], applicability[6]]

        select_rrmse = rrmse[1]
        new_d = self.data
        profile = new_d.iloc[:, [0]] * select_rrmse

        return profile

    def _rrmse_last_working_profile(self, p, d, applicability):
        """
        Return profile with rrmse_last_working.
        :return: output DataFrame
        """
        rrmse = [applicability[2],applicability[4], applicability[6]]

        select_rrmse = rrmse[2]
        new_d = self.data
        profile = new_d.iloc[:, [0]] * select_rrmse

        return profile

    def _get_only_adjustment_hours(self, full_lines):
        modified_lines = full_lines.iloc[self.adjustment_hours]
        return modified_lines

    def _adjust_baseline(self, baselines, adjustments):
        adjusted = baselines.copy()
        for column in baselines.columns:
            adjusted[column] = self._adjust(adjustments[column], adjusted[column])
        return adjusted

    def _get_adjustments_all(self, baselines, original_lines):
        adj0 = original_lines.iloc[self.peak_hours[0]] - baselines.iloc[self.peak_hours[0]]
        adj1 = original_lines.iloc[self.peak_hours[1]] - baselines.iloc[self.peak_hours[1]]
        return (adj0 + adj1) / 2

    def _adjustments_remove_last_working(self, adjustments_all):
        adj_last_working = adjustments_all.copy()
        dates_index = adj_last_working.index
        for date in dates_index.values:
            if self._is_previous_date_weekend_or_exception(date):
                adj_last_working[date] = 0
        return adj_last_working

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
            if len(self.peak_hours) > 2:
                self.logger.error("'peak_hours' must have 2 elements")
                raise Exception("'peak_hours' must have 2 elements")
        except Exception as err:
            self.logger.error("Wrong parameter 'peak_hours': " + str(self.parameters['peak_hours']) + " " + str(err))
            raise Exception("Wrong parameter 'peak_hours': " + str(self.parameters['peak_hours']) + " " + str(err))

    def _check_adjustment_hours(self):
        try:
            self.adjustment_hours = [int(i) for i in self.parameters['adjustment_hours']]
            self.logger.debug("Parsed parameter 'adjustment_hours': " + str(self.adjustment_hours))
            for i in self.adjustment_hours:
                if i > 23:
                    raise Exception("Wrong value in 'adjustment_hours', values must be in range(0, 24)")
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

    def _is_exception_day(self, day):
        """
        Checks if date is contained in exceptions

        :param day: date
        :return: flag
        """
        if day in self.exception_days:
            return True
        return False

    def _is_previous_date_weekend_or_exception(self, day):
        td = datetime.timedelta(days=1)
        dow = day.weekday()
        if self._is_weekend(day - td) and self.except_weekends:
            return True
        if self._is_exception_day(day - td):
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
            if self._is_exception_day(day):
                continue
            days.append(day)

        return days

    def _calculate_baselines(self, days_for_baselines):
        baselines_df = None
        for date, days_avg in days_for_baselines.items():

            data_for_date = self.data.loc[self.data['date'].isin(days_avg)]
            baseline_for_date = data_for_date.groupby(['time']).mean()

            if baselines_df is None:
                baselines_df = pd.DataFrame(index=baseline_for_date.index)

            baselines_df[date] = baseline_for_date

        return baselines_df

    def _calculate_baselines_dates(self, days_for_baselines):
        baselines_df = None
        baseline_full_date = pd.DataFrame()
        for date, days_avg in days_for_baselines.items():

            data_for_date = self.data.loc[self.data['date'].isin(days_avg)]
            baseline_for_date = data_for_date.groupby(['time']).mean()

            if baselines_df is None:
                baselines_df = pd.DataFrame(index=baseline_for_date.index)
            # baseline_full_date = None
            if baseline_full_date is None:
                # new_index = date + baseline_for_date.index

                baseline_full_date = pd.DataFrame(index=pd.date_range(start=date, periods=24, freq='1H'))

            baselines_df[date] = baseline_for_date
            add = baseline_for_date
            idx = pd.date_range(start=date, periods=24, freq='1H')
            add['dtime'] = idx
            add = add.set_index(['dtime'])
            baseline_full_date = baseline_full_date.append(add)
        return baseline_full_date

    def _get_original_lines(self, days_to_analyze):
        original_lines_df = None
        for day in days_to_analyze:
            data_for_date = self.data.loc[self.data['date'] == day]
            if original_lines_df is None:
                original_lines_df = pd.DataFrame(index=data_for_date['time'])
            original_lines_df[day] = list(data_for_date[data_for_date.columns[0]])
        return original_lines_df

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
        format results for output, converts matrix to series
        stacks values in top-down left-right order

        :return: formatted results
        """
        try:
            idx = pd.date_range('2000-01-01', periods=len(res))  # dummy index
            return pd.DataFrame(res, idx, ['value'])
        except Exception as err:
            self.logger.error("Output preparation: " + str(err))
            raise Exception("Output preparation: " + str(err))
