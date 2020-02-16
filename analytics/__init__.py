from .analysis_test import TestAnalysis
from .demand_response.anylysis_demand_response_baseline import DemandResponseAnalysisBaseline
from .demand_response.anylysis_demand_response_discharge import DemandResponseAnalysisDischarge
from .demand_response.anylysis_demand_response_rrmse import DemandResponseAnalysisRRMSE
from .demand_response.anylysis_demand_response_deviation import DemandResponseAnalysisDeviation
from .demand_response.anylysis_demand_response_boolean import DemandResponseAnalysisBoolean
from .demand_response.anylysis_demand_response_check import DemandResponseAnalysisCheck
from .demand_response.anylysis_demand_response_expected import DemandResponseAnalysisExpected
from .peak_prediction.analysis_peak_prediction_statistical import PeakPredictionStatisticalAnalysis
from .peak_prediction.analysis_peak_prediction_ml import PeakPredictionMLAnalysis
from .statistics.analysis_statistics_normalization import SatisticsNormalizationAnalysis
from .correlation.analysis_correlation import CorrelationAnalysis
from .prediction.analysis_prediction_holt_winters import PredictionHoltWintersAnalysis
from analytics._in_development.analysis_prediction_holt_winters_auto import PredictionHoltWintersAutoAnalysis
from analytics._in_development.analysis_evaluation_brutlag import EvaluationBrutlagAnalysis
from analytics._in_development.analysis_autocorrelation import AutocorrelationAnalysis
import logging

logger = logging.getLogger('insyte_analytics.analytics.__init__')
# list of existing analysis functions
ANALYSIS = {
        'test': TestAnalysis,
        'demand-response-baseline': DemandResponseAnalysisBaseline,
        'demand-response-discharge': DemandResponseAnalysisDischarge,
        'demand-response-rrmse': DemandResponseAnalysisRRMSE,
        'demand-response-deviation': DemandResponseAnalysisDeviation,
        'demand-response-boolean': DemandResponseAnalysisBoolean,
        'demand-response-check': DemandResponseAnalysisCheck,
        'demand-response-expected': DemandResponseAnalysisExpected,
        'peak-prediction-statistical': PeakPredictionStatisticalAnalysis,
        'peak-prediction-ml': PeakPredictionMLAnalysis,
        'correlation': CorrelationAnalysis,
        'normalization': SatisticsNormalizationAnalysis,
        'prediction-holt-winters': PredictionHoltWintersAnalysis,
        # in development
        'autocorrelation': AutocorrelationAnalysis,
        'prediction-holt-winters-auto': PredictionHoltWintersAutoAnalysis,
        'evaluation-brutlag': EvaluationBrutlagAnalysis
    }


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
        result = _analysis_caller(analysis, arguments, data_frame)
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
    :return: dataframe
    """
    logger.debug("Starting '" + str(analysis) + "' Influx analysis, parameters: " + str(arguments))
    try:
        result = _analysis_caller(analysis, arguments, data_frame)
    except Exception as err:
        logger.error("Analysis failed: " + str(err))
        raise Exception("Analysis failed: " + str(err))
    logger.debug("Analysis successfully complete")
    return result


def analyze_none(analysis, arguments):
    """
    Calls analysis functions, returns analysis result.

    :param analysis: analysis function name
    :param arguments: dictonary of analysis function arguments
    :return: None
    """
    logger.debug("Starting '" + str(analysis) + "' No-db analysis, parameters: " + str(arguments))
    try:
        result = _analysis_caller(analysis, arguments, None)
    except Exception as err:
        logger.error("Analysis failed: " + str(err))
        raise Exception("Analysis failed: " + str(err))
    logger.debug("Analysis successfully complete")
    return result


def _analysis_caller(analysis, arguments, data_frame):
    """
    Caller function
    """
    if analysis in ANALYSIS:
        result = ANALYSIS[analysis](arguments, data_frame).analyze()
    else:
        logger.error("Analysis function doesn't exist: " + analysis)
        raise Exception("Analysis function doesn't exist: " + analysis)

    return result
