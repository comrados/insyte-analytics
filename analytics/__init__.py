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
# dict of existing analysis functions
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


def check_analysis(analysis_name):
    """
    Checks if analysis exists (presented in list)

    :param analysis_name:
    :return:
    """
    logger.debug("analysis in ANALYSIS: " + str(analysis_name in ANALYSIS))
    return analysis_name in ANALYSIS


def analyze_cassandra(analysis_name, analysis_arguments, loaded_data):
    """
    Calls analysis functions, returns analysis result.

    :param analysis_name: analysis function name
    :param analysis_arguments: dictonary of analysis function arguments
    :param loaded_data: dataframe with data (time series)
    :return: list of tuples for writing [(date1, value1), (date2, value2), ..., (dateN, valueN)]
    """
    logger.debug("Starting '" + str(analysis_name) + "' Cassandra analysis, parameters: " + str(analysis_arguments))
    try:
        result = _analysis_caller(analysis_name, analysis_arguments, loaded_data)
    except Exception as err:
        logger.error("Analysis failed: " + str(err))
        raise Exception("Analysis failed: " + str(err))
    logger.debug("Analysis successfully complete")
    # Reset index
    result.reset_index(inplace=True)
    return [tuple(x.values()) for x in result.to_dict('records')]


def analyze_influx(analysis_name, analysis_arguments, loaded_data):
    """
    Calls analysis functions, returns analysis result.

    :param analysis_name: analysis function name
    :param analysis_arguments: dictonary of analysis function arguments
    :param loaded_data: dataframe with data (time series)
    :return: dataframe
    """
    logger.debug("Starting '" + str(analysis_name) + "' Influx analysis, parameters: " + str(analysis_arguments))
    try:
        result = _analysis_caller(analysis_name, analysis_arguments, loaded_data)
    except Exception as err:
        logger.error("Analysis failed: " + str(err))
        raise Exception("Analysis failed: " + str(err))
    logger.debug("Analysis successfully complete")
    return result


def analyze_no_db(analysis_name, analysis_arguments):
    """
    Calls analysis functions, returns analysis result.

    :param analysis_name: analysis function name
    :param analysis_arguments: dictonary of analysis function arguments
    :return: None
    """
    logger.debug("Starting '" + str(analysis_name) + "' No-db analysis, parameters: " + str(analysis_arguments))
    try:
        result = _analysis_caller(analysis_name, analysis_arguments, None)
    except Exception as err:
        logger.error("Analysis failed: " + str(err))
        raise Exception("Analysis failed: " + str(err))
    logger.debug("Analysis successfully complete")
    return result


def _analysis_caller(analysis_name, analysis_arguments, loaded_data):
    """
    Caller function
    """
    if analysis_name in ANALYSIS:
        result = ANALYSIS[analysis_name](analysis_arguments, loaded_data).analyze()
    else:
        logger.error("Analysis function doesn't exist: " + analysis_name)
        raise Exception("Analysis function doesn't exist: " + analysis_name)

    return result
