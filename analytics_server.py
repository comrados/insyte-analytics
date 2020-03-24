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
from db import InfluxServerIO

import analytics.utils as u
import analytics


def parse_args(args):
    """
    Arguments parser.

    :param args: arguments
    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Analytics Server", prog="analytics_server.py")

    # server
    server_group = parser.add_argument_group('Server', "Server's settings")
    server_group.add_argument("-sh", "--srv-host", dest="srv_host", default="localhost", help="server's host address")
    server_group.add_argument("-sp", "--srv-port", dest="srv_port", default=65000, type=int, help="server's port")

    # database
    dbc_group = parser.add_argument_group("Database", "Database's settings")

    dbc_group.add_argument('-dbh', '--db-host', dest='db_host', nargs='+', default='ems.insyte.ru', help='DB host')
    dbc_group.add_argument('-dbp', '--db-port', dest='db_port', type=int, default=8086, help='DB port')
    dbc_group.add_argument('-dbn', '--db-name', dest='db_name', default='ems', help='DB name')
    dbc_group.add_argument('-dbu', '--db-user', dest='db_user', default='ems_user', help='DB username')
    dbc_group.add_argument('-dbpw', '--db-password', dest='db_password', default=r"4rERYTPhfTtvU!99",
                           help='DB password')

    # logger
    log_group = parser.add_argument_group("Logger", "Logger's settings")

    log_group.add_argument('-lf', '--log-file', dest='log_file', default="analytics_server.log", help='Log file')
    log_group.add_argument('-ll', '--log-level', dest='log_level', default=20, type=int, help='Logging level')
    log_group.add_argument('-lgmt', '--log-gmt', dest='log_gmt', default=True, action='store_true',
                           help='Toggle logger GMT time')

    try:
        parsed = parser.parse_args(args)
    except argparse.ArgumentError:
        parser.print_help()
        sys.exit(2)
    else:
        return parsed


def init_logger(log_file, log_level, log_gmt):
    """
    Initialize logger. Logging is thread safe.

    :param log_file: log file path
    :param log_level: logging level https://docs.python.org/3.7/library/logging.html#logging-levels
    :param log_gmt: set timezone to GMT
    :return: Logger object
    """
    f = '%(asctime)s.%(msecs)d %(levelname)s %(module)s.%(funcName)s %(threadName)s (%(thread)d) %(message)s'
    configs = {'filemode': 'a', 'format': f, 'datefmt': '%Y-%m-%d %H:%M:%S', 'level': log_level,
               'filename': os.path.join('logs', log_file)}
    if not os.path.exists('logs'):
        os.makedirs('logs')
    logging.basicConfig(**configs)
    if log_gmt:
        logging.Formatter.converter = time.gmtime
    return logging.getLogger("analytics_server")


class AnalyticsServer(HTTPServer):
    """
    Server instance.
    """

    def __init__(self, request_handler_class, settings):
        self.ctn = threading.current_thread()
        self.s = settings
        super().__init__((self.s.srv_host, self.s.srv_port), request_handler_class)

    def start(self):
        s = "Analytics Server's address: " + 'http://' + self.s.srv_host + ':' + str(self.s.srv_port)
        print(s)
        logger.info(s)
        self.serve_forever()


class AnalyticsServerThreaded(ThreadingMixIn, AnalyticsServer):
    """
    Threading enabler.
    """
    pass


class AnalyticsRequestHandler(BaseHTTPRequestHandler):
    """
    Request handler.
    """

    def __init__(self, request, client_address, server):
        self.s = server.s
        self.ctn = threading.current_thread()
        self.time = datetime.datetime.utcnow()
        self.json = None  # analysis request
        self.influx = InfluxServerIO(self.s.db_host, self.s.db_name, self.s.db_port, self.s.db_user, self.s.db_password)

        super().__init__(request, client_address, server)

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Service alive! Active threads: " + bytes(str(threading.active_count()), 'utf-8'))

    def do_POST(self):
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
            # self.end_headers()
            # self.wfile.flush()
        except Exception as err:
            self.send_response(400)
            self.influx.disconnect()
            logger.error("Something went wrong: " + str(err))

    def log_message(self, format, *args):
        pass

    def _content_to_json(self, content):
        try:
            self.json = json.loads(content)
            logger.info("JSON content: " + str(self.json))
        except Exception as err:
            logger.error("Impossible to process sent data (not a JSON): " + str(content))
            raise Exception(err)

    def _read_data(self):
        try:
            db_io = self.json["db_io_parameters"]
            tu, di, dsi = self._check_reading_lengths(db_io['time_upload'], db_io['device_id'], db_io['data_source_id'])
            if 'r' in db_io['mode']:
                self.influx.connect()
                self.input = self.influx.read_data(di, dsi, tu, db_io['limit'])
                self.influx.disconnect()
                logger.info("Data has been successfully read from DB: " + str(self.input.shape) + " (data shape)")
        except Exception as err:
            logger.error("Failed to read the data: " + str(err))
            raise Exception(err)

    def _call_analysis(self):
        try:
            ap = self.json["analysis_parameters"]
            self.output = analytics.analyze_influx(ap['analysis'], ap['analysis_arguments'], self.input)
        except Exception as err:
            logger.error("Failed to analyze the data: " + str(err))
            raise Exception(err)

    def _write_results(self):
        try:
            db_io = self.json["db_io_parameters"]
            self._check_write_parameters(db_io['result_id'], self.output)
            if 'w' in db_io['mode']:
                self.influx.connect()
                output_results = self.influx.write_data(db_io['result_id'], self.output)
                self.influx.disconnect()
                logger.info("Data has been saved into DB: " + str(output_results))
        except Exception as err:
            logger.error("Failed to write the data: " + str(err))
            raise Exception(err)

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


if __name__ == "__main__":

    a = parse_args(sys.argv[1:])

    logger = init_logger(a.log_file, a.log_level, a.log_gmt)

    logger.info("Server started: " + str(vars(a)))

    srv = AnalyticsServerThreaded(AnalyticsRequestHandler, a)
    srv_thread = threading.Thread(target=srv.start, daemon=True)
    try:
        srv_thread.start()
        while True:
            continue
    except KeyboardInterrupt:
        logger.info("Server stopped by user\n\n")
        print('Server stopped by user')
        try:
            logging.shutdown()
            srv.shutdown()
            srv.server_close()
            sys.exit(1)
        except Exception as err:
            logger.critical("Interruption error, program was closed with error: ", err)
            os._exit(1)
