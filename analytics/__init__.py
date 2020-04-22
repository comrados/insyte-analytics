import logging
import os
import importlib
import sys


class AnalyticsModule:
    logger = logging.getLogger('analytics')

    def __init__(self, script_folders):
        self.script_folders = script_folders
        self.ANALYSIS, self.ANALYSIS_ARGS = self._import_analysis_functions()

    def run_analysis(self, analysis_name, analysis_arguments, loaded_data):
        """
        Calls analysis functions, returns analysis result.

        :param analysis_name: analysis function name
        :param analysis_arguments: dictonary of analysis function arguments
        :param loaded_data: dataframe with data (time series)
        :return: dataframe
        """
        self.logger.debug(
            "Starting '" + str(analysis_name) + "' Influx analysis, parameters: " + str(analysis_arguments))
        try:
            result = self._analysis_caller(analysis_name, analysis_arguments, loaded_data)
        except Exception as exc:
            self.logger.error("Analysis failed: " + str(exc))
            raise Exception("Analysis failed: " + str(exc))
        self.logger.debug("Analysis successfully complete")
        return result

    def _analysis_caller(self, analysis_name, analysis_arguments, loaded_data):
        """
        Caller function
        """
        try:
            if analysis_name in self.ANALYSIS:
                result = self.ANALYSIS[analysis_name]().analyze(analysis_arguments, loaded_data)
            else:
                self.logger.error("Analysis function doesn't exist: " + analysis_name)
                raise Exception("Analysis function doesn't exist: " + analysis_name)
        except Exception as exc:
            self.logger.error(str(exc))
            raise Exception(str(exc))

        return result

    def update_analysis_functions(self):
        self.ANALYSIS, self.ANALYSIS_ARGS = self._import_analysis_functions()

    def _import_analysis_functions(self):
        # import all scripts from 'analytics/scripts' and 'analytics/_in_development' folders
        analytics_dir = "analytics"
        analysis_scripts_dir = os.path.join(analytics_dir, "scripts")
        in_development_scripts_dir = os.path.join(analytics_dir, "_in_development")

        folders = [analysis_scripts_dir, in_development_scripts_dir, *self.script_folders]

        analysis = {}  # name : class
        analysis_args = {}  # name : analysis arguments

        # loop through directories with scripts
        for folder in folders:
            if os.path.isdir(folder):
                file_list = os.listdir(folder)
                sys.path.append(folder)
                for file in file_list:
                    name, ext = os.path.splitext(file)
                    if name.startswith("analysis_"):
                        # try to import and update dictionaries
                        try:
                            # module_name = folder.replace(r'/', r'.') + "." + name
                            # i = importlib.import_module(module_name)
                            i = importlib.import_module(name)
                            # imported module constants
                            a_name = getattr(i, "ANALYSIS_NAME")
                            a_class_name = getattr(i, "CLASS_NAME")
                            a_class = getattr(i, a_class_name)
                            a_args = getattr(i, "A_ARGS")
                            a_args["folder"] = folder
                            # update dictionaries
                            analysis[a_name] = a_class
                            analysis_args[a_name] = a_args
                            self.logger.debug("Module imported: " + name)
                        except Exception as err:
                            self.logger.error("Failed to import module: " + name + ", " + str(err))
            else:
                self.logger.warning("Impossible import from folder: " + folder + " " + str(err))
        return analysis, analysis_args

    ################################################## OLD FUNCTIONS ###################################################

    def analyze_no_db(self, analysis_name, analysis_arguments):
        """
        Calls analysis functions, returns analysis result.

        :param analysis_name: analysis function name
        :param analysis_arguments: dictonary of analysis function arguments
        :return: None
        """
        self.logger.debug(
            "Starting '" + str(analysis_name) + "' No-db analysis, parameters: " + str(analysis_arguments))
        try:
            result = self._analysis_caller(analysis_name, analysis_arguments, None)
        except Exception as exc:
            self.logger.error("Analysis failed: " + str(exc))
            raise Exception("Analysis failed: " + str(exc))
        self.logger.debug("Analysis successfully complete")
        return result

    def check_analysis(self, analysis_name):
        """
        Checks if analysis exists (presented in list)

        :param analysis_name:
        :return:
        """
        self.logger.debug("analysis in ANALYSIS: " + str(analysis_name in self.ANALYSIS))
        return analysis_name in self.ANALYSIS

    def analyze_cassandra(self, analysis_name, analysis_arguments, loaded_data):
        """
        Calls analysis functions, returns analysis result.

        :param analysis_name: analysis function name
        :param analysis_arguments: dictonary of analysis function arguments
        :param loaded_data: dataframe with data (time series)
        :return: list of tuples for writing [(date1, value1), (date2, value2), ..., (dateN, valueN)]
        """
        self.logger.debug("Starting '" + str(analysis_name) + "' Cassandra parameters: " + str(analysis_arguments))
        try:
            result = self._analysis_caller(analysis_name, analysis_arguments, loaded_data)
        except Exception as exc:
            self.logger.error("Analysis failed: " + str(exc))
            raise Exception("Analysis failed: " + str(exc))
        self.logger.debug("Analysis successfully complete")
        # Reset index
        result.reset_index(inplace=True)
        return [tuple(x.values()) for x in result.to_dict('records')]
