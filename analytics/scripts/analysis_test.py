from analytics.analysis import Analysis

"""
Test analysis function.
"""

CLASS_NAME = "TestAnalysis"
ANALYSIS_NAME = "test"
A_ARGS = {"analysis_code": "TEST",
          "analysis_name": ANALYSIS_NAME,
          "input": "Data of arbitrary dimensionality (N time series)",
          "action": "Increases/decreases/multiplies/divides all input data by specified 'value'",
          "output": "Data of the dimensionality similar to the input one (N modified time series)",
          "mode": "rw",
          "inputs_count": -1,
          "outputs_count": -1,
          "inputs_outputs_always_same_count": True,
          "parameters": [
              {"name": "operation", "count": 1, "type": "SELECT", "options": ["add", "sub", "mul", "div"],
               "info": "operation to execute: add, subtract, multiply or divide"},
              {"name": "value", "count": 1, "type": "FLOAT", "info": "value"}
          ]}


class TestAnalysis(Analysis):

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

    def _parse_parameters(self, parameters):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters")
        try:
            return {'operation': self._check_operation(parameters),
                    'value': self._check_value(parameters)}
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

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

    def _check_operation(self, parameters):
        """
        Checks 'operation' parameter
        """
        if parameters['operation'][0] in ['sub', 'add', 'div', 'mul']:
            self.logger.debug("Parsed parameter 'operation': " + str(parameters['operation'][0]))
            return parameters['operation'][0]
        else:
            self.logger.error("Wrong parameter 'operation': " + str(parameters['operation']))
            raise Exception("Wrong parameter 'operation': " + str(parameters['operation']))

    def _check_value(self, parameters):
        """
        Checks 'value' parameter
        """
        try:
            self.logger.debug("Parsed parameter 'value': " + str(float(parameters['value'][0])))
            return float(parameters['value'][0])
        except Exception as err:
            self.logger.error("Wrong parameter 'value': " + str(parameters['value']) + " " + str(err))
            raise Exception("Wrong parameter 'value': " + str(parameters['value']) + " " + str(err))

    def _prepare_for_output(self, p, d, res):
        """
        Postprocesses DataFrame
        """
        try:
            new_names = {col: ('val' + str(i)) for i, col in enumerate(res.columns)}
            res.rename(columns=new_names, inplace=True)
            return res
        except Exception as err:
            self.logger.error("Output preparation: " + str(err))
            raise Exception("Output preparation: " + str(err))

    def _analyze(self, p, d):
        """
        Run analysis.
        This test function adds, subtracts, multiplies or divides by value all elements of DataFrame

        :return: output DataFrame
        """
        try:
            if p['operation'] == 'sub':
                self.logger.debug("Subtracting: " + str(p['value']))
                d = d.sub(p['value'])
            elif p['operation'] == 'add':
                self.logger.debug("Adding: " + str(p['value']))
                d = d.add(p['value'])
            elif p['operation'] == 'mul':
                self.logger.debug("Multiplying by: " + str(p['value']))
                d = d.mul(p['value'])
            elif p['operation'] == 'div':
                self.logger.debug("Dividing by: " + str(p['value']))
                d = d.div(p['value'])
            else:
                raise Exception("Unknown operation: " + str(p['operation']))
            return d
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))
