from .test import TestAnalysis
import logging


logger = logging.getLogger('insyte_analytics.analytics.__init__')
# list of existing analysis functions
ANALYSIS = ['test']


def check_analysis(analysis):
    """
    Checks if analysis exists (presented in list)

    :param analysis:
    :return:
    """
    logger.debug("analysis in ANALYSIS: " + str(analysis in ANALYSIS))
    return analysis in ANALYSIS


def analyze_cassandra(analysis, arguments, data_frame):
    """
    Calls analysis functions, returns analysis result.

    :param analysis: analysis function name
    :param arguments: dictonary of analysis function arguments
    :param data_frame: dataframe with data (time series)
    :return: list of tuples for writing [(date1, value1), (date2, value2), ..., (dateN, valueN)]
    """
    logger.debug("Starting '" + str(analysis) + "' Cassandra analysis, parameters: " + str(arguments))
    try:
        if analysis == 'test':
            result = TestAnalysis().analyze(arguments, data_frame)
        else:
            logger.error("Analysis function doesn't exist: " + analysis)
            raise Exception("Analysis function doesn't exist: " + analysis)
    except Exception as err:
        logger.error("Analysis failed: " + str(err))
        raise Exception("Analysis failed: " + str(err))
    logger.debug("Analysis successfully complete")
    # Reset index
    result.reset_index(inplace=True)
    return [tuple(x.values()) for x in result.to_dict('records')]


def analyze_influx(analysis, arguments, data_frame):
    """
    Calls analysis functions, returns analysis result.

    :param analysis: analysis function name
    :param arguments: dictonary of analysis function arguments
    :param data_frame: dataframe with data (time series)
    :return: list of tuples for writing [(date1, value1), (date2, value2), ..., (dateN, valueN)]
    """
    logger.debug("Starting '" + str(analysis) + "' Influx analysis, parameters: " + str(arguments))
    try:
        if analysis == 'test':
            result = TestAnalysis().analyze(arguments, data_frame)
        else:
            logger.error("Analysis function doesn't exist: " + analysis)
            raise Exception("Analysis function doesn't exist: " + analysis)
    except Exception as err:
        logger.error("Analysis failed: " + str(err))
        raise Exception("Analysis failed: " + str(err))
    logger.debug("Analysis successfully complete")
    return result
