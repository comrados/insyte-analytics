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
    configs = {'filemode': 'a', 'format': '%(asctime)s.%(msecs)d %(levelname)s %(module)s.%(funcName)s %(message)s',
               'datefmt': '%Y-%m-%d %H:%M:%S', 'level': log_level, 'filename': os.path.join('logs', log_file)}
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
        self.json = None

        super().__init__(request, client_address, server)

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Service alive! Active threads: " + bytes(str(threading.active_count()), 'utf-8'))

    def do_POST(self):
        cl = int(self.headers['Content-Length'])
        content = self.rfile.read(cl)
        logger.info(u.log_msg(str(cl) + " bytes from " + self.client_address[0] + ':' + str(self.client_address[1])))

        try:
            self.json = json.loads(content)
        except:
            self.send_response(400)
            s = 'Impossible to process sent data (not a JSON)'
            print(s)
            logger.error(s)

        db = InfluxServerIO(self.s.db_host, self.s.db_name, self.s.db_port, self.s.db_user, self.s.db_password)

        self.end_headers()
        self.wfile.flush()

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":

    a = parse_args(sys.argv[1:])

    logger = init_logger(a.log_file, a.log_level, a.log_gmt)

    logger.info(u.log_msg("Server started: " + str(vars(a))))

    srv = AnalyticsServerThreaded(AnalyticsRequestHandler, a)
    srv_thread = threading.Thread(target=srv.start, daemon=True)
    try:
        srv_thread.start()
        while True:
            continue
    except KeyboardInterrupt:
        logger.info(u.log_msg("Server stopped by user\n\n"))
        print('Server stopped by user')
        try:
            logging.shutdown()
            srv.shutdown()
            srv.server_close()
            sys.exit(1)
        except Exception as err:
            print("Interruption error: ", err)
            os._exit(1)
