import argparse
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import threading
import datetime
import logging


def parse_args(args):
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

    try:
        parsed = parser.parse_args(args)
    except argparse.ArgumentError:
        parser.print_help()
        sys.exit(2)
    else:
        return parsed


class AnalyticsServer(HTTPServer):

    def __init__(self, request_handler_class, settings):
        self.ctn = threading.current_thread()
        self.s = settings
        super().__init__((self.s.srv_host, self.s.srv_port), request_handler_class)

    def start(self):
        s = "Analytics Server's address: " + 'http://' + self.s.srv_host + ':' + str(self.s.srv_port)
        print(s)
        # TODO add to logs
        self.serve_forever()


class AnalyticsServerThreaded(ThreadingMixIn, AnalyticsServer):
    pass


class AnalyticsRequestHandler(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server):
        self.s = server.s
        self.ctn = threading.current_thread()
        self.time = datetime.datetime.utcnow()

        super().__init__(request, client_address, server)

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Service alive! Active threads: " + bytes(str(threading.active_count()), 'utf-8'))

    def do_POST(self):
        pass

    def log_message(self, format, *args):
        pass


async def main(arg):

    # TODO logging

    database = {'influx': influx, 'cassandra': cassandra, 'none': none}

    global logger
    logger = init_logger(arg.log, arg.log_path, arg.log_level, arg.result_id)
    logger.info("Session started")
    try:
        # Check and modify args
        await check_args(arg)

        # execute db read, analysis, db write routine fro specified database
        await database[arg.connection_type](arg)

        logger.info("Session successfully ended")
        print("DONE")
    except Exception as err:
        print("ERROR: " + str(err))
        logger.error("Session ended with error: " + str(err))


if __name__ == "__main__":

    all_settings = parse_args(sys.argv[1:])

    srv = AnalyticsServerThreaded(AnalyticsRequestHandler, all_settings)
    srv_thread = threading.Thread(target=srv.start, daemon=True)
    try:
        srv_thread.start()
        while True:
            continue
    except KeyboardInterrupt:
        print('Stopped by user')
        try:
            srv.shutdown()
            srv.server_close()
            sys.exit(1)
        except Exception as err:
            print("Interruption error: ", err)
            os._exit(1)
