import logging
import os
import importlib

logger = logging.getLogger('analytics')


def import_functions():
    # import all scripts from 'analytics/scripts' and 'analytics/_in_development' folders
    analytics_dir = "analytics"
    analysis_scripts_dir = "scripts"
    in_development_scripts_dir = "_in_development"

    analysis = {}  # name : class
    analysis_args = {}  # name : analysis arguments

    # loop through directories with scripts
    for scripts_dir in [analysis_scripts_dir, in_development_scripts_dir]:
        scripts_list = os.listdir(os.path.join(analytics_dir, scripts_dir))
        for script in scripts_list:
            name, ext = os.path.splitext(script)
            if name.startswith("analysis_"):
                # try to import and update dictionaries
                try:
                    i = importlib.import_module(analytics_dir + "." + scripts_dir + "." + name)
                    # imported module constants
                    a_name = getattr(i, "ANALYSIS_NAME")
                    a_class_name = getattr(i, "CLASS_NAME")
                    a_class = getattr(i, a_class_name)
                    a_args = getattr(i, "A_ARGS")
                    a_args["folder"] = scripts_dir
                    # update dictionaries
                    analysis[a_name] = a_class
                    analysis_args[a_name] = a_args
                    logger.debug("Module imported: " + name)
                except Exception as err:
                    logger.error("Failed to import module: " + name + ", " + str(err))
    return analysis, analysis_args


ANALYSIS, ANALYSIS_ARGS = import_functions()
print()


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
    except Exception as exc:
        logger.error("Analysis failed: " + str(exc))
        raise Exception("Analysis failed: " + str(exc))
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
    except Exception as exc:
        logger.error("Analysis failed: " + str(exc))
        raise Exception("Analysis failed: " + str(exc))
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
    except Exception as exc:
        logger.error("Analysis failed: " + str(exc))
        raise Exception("Analysis failed: " + str(exc))
    logger.debug("Analysis successfully complete")
    return result


def _analysis_caller(analysis_name, analysis_arguments, loaded_data):
    """
    Caller function
    """
    try:
        if analysis_name in ANALYSIS:
            result = ANALYSIS[analysis_name]().analyze(analysis_arguments, loaded_data)
        else:
            logger.error("Analysis function doesn't exist: " + analysis_name)
            raise Exception("Analysis function doesn't exist: " + analysis_name)
    except Exception as exc:
        logger.error(str(exc))
        raise Exception(str(exc))

    return result
