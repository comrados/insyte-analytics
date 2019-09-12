import logging
import pandas as pd
from .analysis import Analysis


class TestAnalysis(Analysis):
    """
    Test analytics function
    """

    logger = logging.getLogger('insyte_analytics.analytics.analysis_test')

    # Input variables
    parameters = None  # analysis function parameters
    data = None  # DataFrame with data

    # Parsed parameters
    operation = None
    value = None

    # Output variables

    def __init__(self, parameters, data):
        super().__init__(parameters, data)
        self.logger.debug("Initialization")

    def _parse_parameters(self):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters")
        try:
            self._check_operation()
            self._check_value()
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _check_operation(self):
        """
        Checks 'operation' parameter
        """
        if self.parameters['operation'][0] in ['sub', 'add', 'div', 'mul']:
            self.operation = self.parameters['operation'][0]
            self.logger.debug("Parsed parameter 'operation': " + str(self.operation))
        else:
            self.logger.debug("Wrong parameter 'operation': " + str(self.operation))
            raise Exception("Wrong parameter 'operation': " + str(self.operation))

    def _check_value(self):
        """
        Checks 'value' parameter
        """
        try:
            self.value = float(self.parameters['value'][0])
            self.logger.debug("Parsed parameter 'value': " + str(self.value))
        except Exception as err:
            self.logger.debug("Wrong parameter 'value': " + str(self.value) + " " + str(err))
            raise Exception("Wrong parameter 'value': " + str(self.value) + " " + str(err))

    def _postprocess_df(self):
        """
        Postprocesses DataFrame
        """
        pass

    def analyze(self):
        """
        Run analysis.
        This test function adds, subtracts, multiplies or divides by value all elements of DataFrame

        :return: output DataFrame
        """
        super().analyze()
        try:
            self._parse_parameters()
            if self.operation == 'sub':
                self.logger.debug("Subtracting: " + str(self.value))
                self.data = self.data.sub(self.value)
            elif self.operation == 'add':
                self.logger.debug("Adding: " + str(self.value))
                self.data = self.data.add(self.value)
            elif self.operation == 'mul':
                self.logger.debug("Multiplying by: " + str(self.value))
                self.data = self.data.mul(self.value)
            elif self.operation == 'div':
                self.logger.debug("Dividing by: " + str(self.value))
                self.data = self.data.div(self.value)
            else:
                raise Exception("Unknown operation: " + str(self.operation))
            self._preprocess_df()
            return self.data
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))
