import pandas as pd
import numpy as np
from analytics.analysis import Analysis

"""
Workload Statistics. Calculation of working and idle times, on/off modes switching.
"""

CLASS_NAME = "WorkloadStatsAnalysis"
ANALYSIS_NAME = "workload_stats"
A_ARGS = {"analysis_code": "WORKLOAD_STATS",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Calculates work and idle times, on and off counts",
          "output": "1 time series (7 values) with dummy index",
          "mode": "rw",
          "inputs_count": 1,
          "outputs_count": 1,
          "inputs_outputs_always_same_count": True,
          "parameters": [
              {"name": "val_high", "count": 1, "type": "FLOAT", "info": "threshold of 'on' mode"},
              {"name": "val_low", "count": 1, "type": "FLOAT", "info": "threshold of 'off' mode"}
          ]}


class WorkloadStatsAnalysis(Analysis):

    def __init__(self):
        super().__init__()
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
            res = self._analyze(p, d)
            out = self._prepare_for_output(p, d, res)
            return out
        except Exception as err:
            self.logger.error(err)
            raise Exception(str(err))

    def _analyze(self, p, d):
        try:
            super()._analyze(p, d)

            idx = np.array(d.index)
            y = np.array(d[d.columns[0]])
            val_high = p['val_high']
            val_low = p['val_low']

            # time deltas
            ds = [idx[i + 1] - idx[i] for i in range(idx.size - 1)]

            # work/idle time calculation (in nanoseconds!)
            t_total = (idx[-1] - idx[0]) / np.timedelta64(1, 'ns')
            t_work, t_idle, t_neutral = self._calculate_work_idle_time(ds, y, val_high, val_low)

            # on/off count
            cs = self._encode_values(y, val_high, val_low)
            c_on, c_off, c_neutral = self._count_on_off(cs)

            # output
            out_arr = [t_total, t_work, t_idle, t_neutral, c_on, c_off, c_neutral]
            self.logger.debug("Output stats: " + str(out_arr) + "\n")
            return out_arr
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))

    @staticmethod
    def _encode_values(y, val_high, val_low):
        """
        Encodes values of time series with regard to border values. 1 - higher than high, -1 - lower than low, 0 - rest

        :param y: time series
        :param val_high: high border value
        :param val_low: low border value
        :return: encoded series
        """

        cs = []

        for v in y:
            if v >= val_high:
                cs.append(1)
            elif v < val_low:
                cs.append(-1)
            else:
                cs.append(0)

        return cs

    @staticmethod
    def _calculate_work_idle_time(ds, y, val_high, val_low):
        """
        calculates working and idle time with regard to border values.
        work - higher than high border
        idle - lower than low border
        neutral - rest

        :param ds: time deltas
        :param y: values
        :param val_high: high border
        :param val_low: low border
        :return: work, idle and neutral times
        """

        t_work = 0
        t_idle = 0
        t_neutral = 0

        for i in range(len(ds)):
            if (y[i] >= val_high) and (y[i + 1] >= val_high):
                t_work += ds[i] / np.timedelta64(1, 'ns')
            elif (y[i] < val_low) and (y[i + 1] < val_low):
                t_idle += ds[i] / np.timedelta64(1, 'ns')
            else:
                t_neutral += ds[i] / np.timedelta64(1, 'ns')

        return t_work, t_idle, t_neutral

    @staticmethod
    def _count_on_off(cs):
        """
        counts switching of working modes
        -1 or 0 -> 1 - on
        1 or 0 -> -1 - off
        1 or -1 -> 0 - neutral

        :param cs: encoded values
        :return:
        """

        c_on = 0
        c_off = 0
        c_neutral = 0

        for i in range(len(cs) - 1):
            if cs[i] != cs[i + 1]:
                if cs[i + 1] == 1:
                    c_on += 1
                elif cs[i + 1] == -1:
                    c_off += 1
                else:
                    c_neutral += 1

        return c_on, c_off, c_neutral

    def _preprocess_df(self, data):
        """
        Preprocesses DataFrame

        Drops row if any of columns has NaN
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs
            if data is not None:
                if data.empty:
                    raise Exception("Empty DataFrame")
                dat = data.dropna(how='any')
                if data.empty:
                    raise Exception("Empty DataFrame after preprocessing")
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
            val_high = self._check_val(parameters, 'val_high')
            val_low = self._check_val(parameters, 'val_low')
            if val_low > val_high:
                raise Exception("'val_low' must be lesser than or equal to 'val_high'")
            return {'val_high': val_high, 'val_low': val_low}
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_val(self, parameters, name):
        """
        Checks 'val_low' and 'val_high' parameters
        """
        try:
            val = float(parameters[name][0])
            self.logger.debug("Parsed parameter '" + name + "': " + str(val))
            return val
        except Exception as err:
            self.logger.error("Wrong parameter '" + name + "': " + str(parameters[name]) + " " + str(err))
            raise Exception("Wrong parameter '" + name + "': " + str(parameters[name]) + " " + str(err))

    def _prepare_for_output(self, p, d, res):
        """
        puts data into DataFrame

        :return: formatted results
        """
        try:

            idx = pd.date_range('2000-01-01', periods=len(res))  # dummy index

            return pd.DataFrame(res, idx, ['value_workload_stats'])
        except Exception as err:
            self.logger.error("Output preparation: " + str(err))
            raise Exception("Output preparation: " + str(err))
