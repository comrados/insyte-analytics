import argparse
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import threading
import datetime
import logging
import time
import json

import analytics
import analytics.utils as u

from db import InfluxServerIO


def parse_args(args):
    """
    Arguments parser.

    :param args: arguments
    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Analytics Server", prog="analytics_server.py")

    # server
    server_group = parser.add_argument_group('Server', "Server's settings")
    server_group.add_argument("-sh", "--srv-host", dest="srv_host", default="92.53.78.60", help="server's host address")
    server_group.add_argument("-sp", "--srv-port", dest="srv_port", default=65000, type=int, help="server's port")
    server_group.add_argument("-sah", "--srv-allowed-hosts", dest="srv_allowed_hosts", default=[], nargs="*",
                              help="list of allowed hosts")
    server_group.add_argument("-sum", "--srv-update-mode", dest="srv_update_mode", default="both",
                              choices=["auto", "manual", "both"], help="analysis functions update modes: "
                                                                       "'auto' - time-interval based automatic, "
                                                                       "'manual' - via GET-request, 'both' - both")
    server_group.add_argument("-saui", "--srv-auto-update-int", dest="srv_auto_update_int", default=900, type=int,
                              help="analysis functions auto update interval (seconds), <= 0 if disabled")
    server_group.add_argument("-ssf", "--srv-script-folders", dest="srv_script_folders", default=[], nargs="*",
                              help="additional analytics script folders with , default ones ('analytics/scripts' "
                                   "and 'analytics/_in_development') will be used in any case")

    # database
    dbc_group = parser.add_argument_group("Database", "Database's settings")

    dbc_group.add_argument('-dbh', '--db-host', dest='db_host', default='ems.insyte.ru', help='DB host')
    dbc_group.add_argument('-dbp', '--db-port', dest='db_port', type=int, default=8086, help='DB port')
    dbc_group.add_argument('-dbn', '--db-name', dest='db_name', default='ems', help='DB name')
    dbc_group.add_argument('-dbu', '--db-user', dest='db_user', default='ems_user', help='DB username')
    dbc_group.add_argument('-dbpw', '--db-password', dest='db_password', default=r"4rERYTPhfTtvU!99",
                           help='DB password')

    # logger
    log_group = parser.add_argument_group("Logger", "Logger's settings")

    log_group.add_argument('-lf', '--log-file', dest='log_file', default="analytics_server.log", help='Log file')
    log_group.add_argument('-ld', '--log-dir', dest='log_dir', default="logs", help='Log directory')
    log_group.add_argument('-ll', '--log-level', dest='log_level', default=20, type=int, help='Logging level')
    log_group.add_argument('-lgmt', '--log-gmt', dest='log_gmt', default=True, action='store_false',
                           help='Disable GMT+0 logging time zone, use local time instead')

    try:
        parsed = parser.parse_args(args)
        parsed.srv_auto_update = (parsed.srv_update_mode in ["auto", "both"]) and (parsed.srv_auto_update_int > 0)
        parsed.srv_manual_update = parsed.srv_update_mode in ["manual", "both"]

    except argparse.ArgumentError:
        parser.print_help()
        sys.exit(2)
    else:
        return parsed


def init_logger(log_file, log_dir, log_level, log_gmt):
    """
    Initialize logger. Logging is thread safe.

    :param log_file: log file name
    :param log_dir: log directory
    :param log_level: logging level https://docs.python.org/3.7/library/logging.html#logging-levels
    :param log_gmt: set timezone to GMT
    :return: Logger object
    """
    f = '%(asctime)s.%(msecs)d %(levelname)s %(module)s.%(funcName)s %(threadName)s (%(thread)d) %(message)s'
    configs = {'filemode': 'a', 'format': f, 'datefmt': '%Y-%m-%d %H:%M:%S', 'level': log_level,
               'filename': os.path.join(log_dir, log_file)}
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(**configs)
    if log_gmt:
        logging.Formatter.converter = time.gmtime
    return logging.getLogger("analytics_server")


def auto_update_analysis_functions(args, analysis_module):
    """
    Updates analysis functions with given frequency (if enabled)

    :param args: parsed args
    :param analysis_module: analysis module instance
    :return:
    """
    if args.srv_auto_update:
        time.sleep(args.srv_auto_update_int)
        analysis_module.update_analysis_functions()
        logger.info("Analysis functions updated automatically")


class AnalyticsServer(HTTPServer):
    """
    Server instance.
    """

    def __init__(self, request_handler_class, settings, analytics_module):
        self.ctn = threading.current_thread()
        self.s = settings
        self.am = analytics_module
        super().__init__((self.s.srv_host, self.s.srv_port), request_handler_class)

    def start(self):
        """
        Start server.
        """
        s = "Analytics Server's address: " + 'http://' + self.s.srv_host + ':' + str(self.s.srv_port)
        print(s)
        logger.info(s)
        self.serve_forever()

    def finish_request(self, request, client_address):
        """Finish one request by instantiating RequestHandlerClass."""
        client = client_address[0] + ':' + str(client_address[1])
        if self._check_host_allowance(client_address):
            logger.info("Request from: " + client)
            self.RequestHandlerClass(request, client_address, self)
        else:
            logger.warning("Ignoring request from unauthorized host: " + client)

    def _check_host_allowance(self, client_address):
        if len(self.s.srv_allowed_hosts) > 0:
            return True if client_address[0] in self.s.srv_allowed_hosts else False
        else:
            return True


class AnalyticsServerThreaded(ThreadingMixIn, AnalyticsServer):
    """
    Threading enabler: inherits form threading class and server class.
    """
    daemon_threads = True  # init request processing threads as daemonic
    block_on_close = True  # wait until the completion of all non-daemonic threads before termination


class AnalyticsRequestHandler(BaseHTTPRequestHandler):
    """
    Request handler.
    """

    server_version = "InsyteAnalyticsServer(" + BaseHTTPRequestHandler.server_version + ")"

    def __init__(self, request, client_address, server):
        self.s = server.s
        self.am = server.am
        self.ctn = threading.current_thread()
        self.time = datetime.datetime.utcnow()
        self.influx = InfluxServerIO(self.s.db_host, self.s.db_name, self.s.db_port, self.s.db_user, self.s.db_password)
        self.json_request = None  # analysis request
        self.input = None
        self.output = None

        self.get_requests = [
            (["/status/", "/status", "/status.json"], self._do_get_status),
            (["/functions/", "/functions", "/functions.json"], self._do_get_functions),
            (["/update_analysis_functions/", "/update_analysis_functions", "/update_analysis_functions.json"],
             self._do_get_update_analysis_functions),
            (["/logs", "/logs/", "/log", "/log/"], self._do_get_log)
        ]

        super().__init__(request, client_address, server)

        print()

    def do_GET(self):
        """
        GET-request processor
        """
        try:
            client = self.client_address[0] + ':' + str(self.client_address[1])
            # addr - addresses, func - function caller
            for addr, func in self.get_requests:
                if self.path in addr:
                    func(client)

        except Exception as err:
            logger.error("GET-failure: " + str(err))
            self.send_response(400)
            msg = {'result': 'ERROR', 'error_message': str(err)}
            self.wfile.write(bytes(json.dumps(msg, indent=4, sort_keys=True), 'utf-8'))

    def _do_get_status(self, client):
        """
        GET 'status' request processor

        :param client: address
        """
        logger.info("GET 'status' request from " + client)
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(bytes(json.dumps(self._get_status_msg(), indent=4, sort_keys=True), 'utf-8'))

    def _do_get_functions(self, client):
        """
        GET 'functions' request processor

        :param client: address
        """
        logger.info("GET 'functions' request from " + client)
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(bytes(json.dumps(self.am.ANALYSIS_ARGS, indent=4, sort_keys=True), 'utf-8'))

    def _do_get_update_analysis_functions(self, client):
        """
        GET 'update_analysis_functions' request processor

        :param client: address
        """
        logger.info("GET 'update_analysis_functions' request from " + client)
        if self.s.srv_manual_update:
            self.am.update_analysis_functions()
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(bytes(json.dumps(self.am.ANALYSIS_ARGS, indent=4, sort_keys=True), 'utf-8'))
            logger.info("Analysis functions updated manually")
        else:
            logger.warning("Manual analysis functions update is disabled")

    def _do_get_log(self, client):
        """
        GET 'functions' request processor

        :param client: address
        """
        logger.info("GET 'log' request from " + client)
        with open(os.path.join(self.s.log_dir, self.s.log_file), "r") as log:
            content = log.read()
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(bytes(content, 'utf-8'))

    def do_POST(self):
        """
        POST-request processor
        """
        try:
            # read POST-request's content
            cl = int(self.headers['Content-Length'])
            content = self.rfile.read(cl)
            logger.info(str(cl) + " bytes from " + self.client_address[0] + ':' + str(self.client_address[1]))

            # analysis parameters
            self._content_to_json(content)

            # analyse
            self._read_data()
            self._call_analysis()
            self._write_results()

            # send response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            msg = {'result': 'DONE', 'active_threads': threading.active_count()}
            self.wfile.write(bytes(json.dumps(msg, indent=4, sort_keys=True), 'utf-8'))
        except Exception as err:
            logger.error("POST-failure: " + str(err))
            self.influx.disconnect()

            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            msg = {'result': 'ERROR', 'error_message': str(err), 'active_threads': threading.active_count()}
            self.wfile.write(bytes(json.dumps(msg, indent=4, sort_keys=True), 'utf-8'))

    def log_message(self, format, *args):
        """
        Disables default logging
        """
        pass

    def _content_to_json(self, content):
        """
        Converts POST-request's content into proper json
        """
        try:
            self.json_request = json.loads(content)
            logger.info("JSON content: " + str(self.json_request))
        except Exception as err:
            logger.error("Impossible to process sent data (not a JSON): " + str(err))
            raise Exception("Impossible to process sent data (not a JSON): " + str(err))

    def _read_data(self):
        """
        Read data for processing
        """
        try:
            db_io = self.json_request["db_io_parameters"]
            if db_io['limit'] == 'null':
                db_io['limit'] = None
            if 'r' in db_io['mode']:
                tu, di, dsi = self._check_reading_lengths(db_io['time_upload'], db_io['device_id'],
                                                          db_io['data_source_id'])
                self.influx.connect()
                self.input = self.influx.read_data(di, dsi, tu, db_io['limit'])
                self.influx.disconnect()
                logger.info("Data has been successfully read from DB: " + str(self.input.shape) + " (rows, columns)")
        except Exception as err:
            logger.error("Failed to read the data: " + str(err))
            raise Exception("Failed to read the data: " + str(err))

    def _call_analysis(self):
        """
        Analysis caller
        """
        try:
            ap = self.json_request["analysis_parameters"]
            self.output = self.am.run_analysis(ap['analysis'], ap['analysis_arguments'], self.input)
        except Exception as err:
            logger.error("Failed to analyze the data: " + str(err))
            raise Exception("Failed to analyze the data: " + str(err))

    def _write_results(self):
        """
        Write analysis results to DB
        """
        try:
            db_io = self.json_request["db_io_parameters"]
            if 'w' in db_io['mode']:
                self._check_write_parameters(db_io['result_id'], self.output)
                self.influx.connect()
                output_results = self.influx.write_data(db_io['result_id'], self.output)
                self.influx.disconnect()
                logger.info("Data has been saved into DB: " + str(self.output.shape) + " " + str(output_results))
        except Exception as err:
            logger.error("Failed to write the data: " + str(err))
            raise Exception("Failed to write the data: " + str(err))

    def _check_reading_lengths(self, time_upload, device_id, data_source_id):
        """
        Checks if lengths of reading parameters equal

        :param time_upload: list of tuples of datetimes [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
        :param device_id: list of uuid objects [uuid1, uuid2, ..., uuidN]
        :param data_source_id: list of integers [id1, id2, ..., idN]
        """
        time_upload = self._format_tu(time_upload)
        lengths = [len(time_upload), len(device_id), len(data_source_id)]
        logger.debug("Lengths of 'time_upload', 'device_id', 'data_source_id': " + str(lengths))
        if len(device_id) != len(data_source_id) or len(device_id) != len(time_upload):
            logger.error("'time_upload', 'device_id', 'data_source_id' have different lengths: " + str(lengths))
            raise Exception("'time_upload', 'device_id', 'data_source_id' have different lengths: " + str(lengths))
        else:
            return time_upload, device_id, data_source_id

    @staticmethod
    def _format_tu(time_upload):
        """
        Checks and reformats time upload argument.

        :param time_upload: list of upload times (strings) [d_min1, d_max1, d_min2, d_max2, ..., d_minN, d_maxN]
        :return: list of tuples of upload times (datetimes) [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
        """
        output = []
        logger.debug("Checking and reformatting 'time_upload': " + str(time_upload))
        if time_upload is None:
            raise Exception("No 'time_upload' provided for 'r' or 'rw' mode")
        if len(time_upload) % 2 == 0:
            try:
                for i in range(0, len(time_upload), 2):
                    d_min = u.string_to_datetime(time_upload[i])
                    d_max = u.string_to_datetime(time_upload[i + 1])
                    output.append((d_min, d_max))
            except Exception as err:
                raise Exception("Impossible to convert to datetime: " + str(err))
        else:
            raise Exception("length of 'time_upload' must be even, current length = " + str(len(time_upload)))
        logger.debug("Modified 'time_upload': " + str(output))
        return output

    @staticmethod
    def _check_write_parameters(result_id, output_data):
        """
        Check writing parameters

        :param result_id:
        :param output_data:
        :return:
        """
        logger.debug("Checking writing parameters")
        if result_id is None:
            logger.warning("Writing parameter 'result_id' not set")
            raise Exception("Writing parameter 'result_id' not set")

        if output_data is None:
            logger.warning("Writing parameter 'output_data' not set")
            raise Exception("Writing parameter 'output_data' not set")

        if output_data.empty:
            logger.warning("'output_data' is empty")
            raise Exception("'output_data' is empty")

        logger.debug("Writing parameters successfully checked")

    @staticmethod
    def _get_status_msg():
        """
        Server's status wrapper

        :return: dictionary with status variables
        """

        def thread_name(t):
            name = t.name + ' (' + str(t.ident) + ')'
            if t.daemon:
                name += ' daemon'
            return name

        threads = [thread_name(t) for t in threading.enumerate()]
        return {"active_threads": threading.active_count(),
                "active_list": threads}


if __name__ == "__main__":
    # parse args
    a = parse_args(sys.argv[1:])

    # init logger
    logger = init_logger(a.log_file, a.log_dir, a.log_level, a.log_gmt)
    logger.info("Server started: " + str(vars(a)))

    # init analytics server
    am = analytics.AnalyticsModule(a.srv_script_folders)
    srv = AnalyticsServerThreaded(AnalyticsRequestHandler, a, am)
    srv_thread = threading.Thread(target=srv.start, daemon=True)

    # main loop
    try:
        srv_thread.start()
        while True:
            auto_update_analysis_functions(a, am)
            continue
    # shutdown
    except KeyboardInterrupt:
        logger.info("Server stopped by user\n\n")
        print('Server stopped by user')
        try:
            logging.shutdown()
            srv.shutdown()
            srv.server_close()
            sys.exit(1)
        except Exception as e:
            logger.critical("Interruption error, program was closed with error: ", e)
            os._exit(1)
