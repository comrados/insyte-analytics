import logging
import pandas as pd


class TestAnalysis:
    """
    Test analytics function
    """

    logger = logging.getLogger('insyte_analytics.analytics.test')

    # Input variables
    parameters = None  # analysis function parameters
    data = None  # DataFrame with data

    # Parsed parameters
    operation = None
    value = None

    # Output variables

    def __init__(self):
        self.logger.debug("Initialization")

    @classmethod
    def _parse_parameters(cls):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        cls.logger.debug("Parsing parameters")
        try:
            cls._check_operation()
            cls._check_value()
        except Exception as err:
            cls.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    @classmethod
    def _check_operation(cls):
        """
        Checks 'operation' parameter
        """
        if cls.parameters['operation'] in ['sub', 'add', 'div', 'mul']:
            cls.operation = cls.parameters['operation']
            cls.logger.debug("Parsed parameter 'operation': " + str(cls.operation))
        else:
            cls.logger.debug("Wrong parameter 'operation': " + str(cls.operation))
            raise Exception("Wrong parameter 'operation': " + str(cls.operation))

    @classmethod
    def _check_value(cls):
        """
        Checks 'value' parameter
        """
        try:
            cls.value = float(cls.parameters['value'])
            cls.logger.debug("Parsed parameter 'value': " + str(cls.value))
        except Exception as err:
            cls.logger.debug("Wrong parameter 'value': " + str(cls.value) + " " + str(err))
            raise Exception("Wrong parameter 'value': " + str(cls.value) + " " + str(err))

    @classmethod
    def analyze(cls, parameters, data):
        """
        Run analysis.
        This test function adds, subtracts, multiplies or divides by value all elements of DataFrame

        :return: output DataFrame
        """
        cls.parameters = parameters
        cls.data = data
        try:
            cls._parse_parameters()
            if cls.operation == 'sub':
                cls.logger.debug("Subtracting: " + str(cls.value))
                cls.data.sub(cls.value)
                return cls.data.sub(cls.value)
            elif cls.operation == 'add':
                cls.logger.debug("Adding: " + str(cls.value))
                cls.data.add(cls.value)
                return cls.data.add(cls.value)
            elif cls.operation == 'mul':
                cls.logger.debug("Multiplying by: " + str(cls.value))
                cls.data.mul(cls.value)
                return cls.data.mul(cls.value)
            elif cls.operation == 'div':
                cls.logger.debug("Dividing by: " + str(cls.value))
                cls.data.div(cls.value)
                return cls.data.div(cls.value)
            else:
                raise Exception("Unknown operation: " + str(cls.operation))
        except Exception as err:
            cls.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))
