import argparse
import sys
import logging
import os
import uuid
import pandas as pd
import analytics
import db
import asyncio
from analytics import utils


def parse_args(argv):
    """
    Parses input arguments from command prompt.

        :param argv: input arguments

        :return: returns namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(description='''Reads data from DB, processes, puts back to DB''',
                                     prog='insyte_analytics.py')
    # Logger
    logger_group = parser.add_argument_group('logger', 'logger parameters')
    logger_group.add_argument('-l', '--log', dest='log', default=False, action='store_true',
                              help='enables file-logging, filename=<result_id>.log')
    logger_group.add_argument('-lp', '--log-path', dest='log_path', default="logs", help='log file output folder')
    logger_group.add_argument('-ll', '--log-level', dest='log_level', default=20, type=int, help='logging level')
    # DB Connection
    dbc_group = parser.add_argument_group('database connection', 'database connection parameters')
    dbc_group.add_argument('-ct', '--connection-type', dest='connection_type', default='influx',
                           choices=['influx', 'cassandra', 'none'],
                           help="database type ('influxdb', 'cassandra' or 'none')")
    dbc_group.add_argument('-ha', '--host-address', dest='host_address', nargs='+', default=['ems.insyte.ru'],
                           help='host addresses')
    dbc_group.add_argument('-db', '--database', dest='database', default='ems', help='database name')
    dbc_group.add_argument('-p', '--port', dest='port', type=int, default=8086, help='Port')
    dbc_group.add_argument('-un', '--username', dest='username', default='ems_user', help='username')
    dbc_group.add_argument('-pw', '--password', dest='password', default=r"4rERYTPhfTtvU!99", help='password')
    dbc_group.add_argument('-m', '--mode', dest='mode', default='rw', choices=['r', 'w', 'rw'],
                           help="db access mode: 'r' - read, 'w' - write, 'rw' - read and write")
    # DB Reading
    dbr_group = parser.add_argument_group('database reading', 'database reading parameters')
    dbr_group.add_argument('-di', '--device-id', dest='device_id', nargs='+', default=None,
                           help='device UUIDs sequence of length N <uuid1 uuid2 ... uuidN>')
    dbr_group.add_argument('-dsi', '--data-source-id', dest='data_source_id', nargs='+', default=None,
                           help='data source IDs sequence of length N <id1 id2 ... idN>')
    dbr_group.add_argument('-tu', '--time_upload', dest='time_upload', nargs='+', default=None,
                           help='dates set of length 2N <d_min1 d_max1 d_min2 d_max2 ... d_minN d_maxN>' +
                                ' in format YYYY-mm-dd_HH:MM:SS±ZZZZ (2018-11-01_00:00:00+0000)')
    dbr_group.add_argument('-lim', '--limit', dest='limit', default=None, type=int,
                           help='limit of retrieved DB entries per query')
    # DB Writing
    dbw_group = parser.add_argument_group('database writing', 'database writing parameters')
    dbw_group.add_argument('-ri', '--result-id', dest='result_id', nargs='+', default=None,
                           help='analysis result UUIDs sequence of length K <uuid1 uuid2 ... uuidK>')
    # Analysis
    analysis_group = parser.add_argument_group('analysis', 'analysis parameters')
    analysis_group.add_argument('-a', '--analysis', dest='analysis', required=True, choices=analytics.ANALYSIS,
                                help='analysis function name')
    analysis_group.add_argument('-aa', '--analysis-args', dest='analysis_args', nargs='*',
                                help='analysis function arguments key-count-value sequences <k1 n1 v11 v12 v13'
                                     'k2 n2 v21 v22 v23 ... kN nN vN1 vN2 vN3>, where kN - Nth key, '
                                     'nN - values count of Nth key, vNi - ith value of Nth key')
    try:
        parsed_args = parser.parse_args(argv)
    except argparse.ArgumentError:
        parser.print_help()
        sys.exit(2)
    else:
        return parsed_args
    finally:
        print()


def init_logger(log_flag, log_path, log_level, result_id):
    """
    Initialize logger.

    :param log_flag: logging flag
    :param log_path: log file path
    :param log_level: logging level https://docs.python.org/3.7/library/logging.html#logging-levels
    :param result_id: log file name
    :return: Logger object
    """
    # log config (if flag is True - output as file, otherwise - in console)
    configs = {'filemode': 'a', 'format': '%(asctime)s.%(msecs)d %(levelname)s %(module)s.%(funcName)s %(message)s',
               'datefmt': '%Y-%m-%d %H:%M:%S', 'level': log_level}
    if log_flag:
        # check log output directory, create if not exists
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        # logging to file additional config
        logname = ""
        if result_id:
            for ri in result_id:
                logname += str(ri) + "_"
        else:
            from datetime import datetime
            time = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')[:-3]
            logname = "error" + time + " "
        logname = logname[0:len(logname) - 1]
        configs['filename'] = os.path.join(log_path, logname + '.log')
    logging.basicConfig(**configs)
    return logging.getLogger("insyte_analytics")


async def check_args(arguments):
    """
    Checks arguments values and modifies data structures/types.

    :param arguments: namespace of parsed arguments
    :return: reformatted arguments
    """
    logger.debug("Checking parsed arguments")
    try:
        if arguments.log and arguments.result_id is None:
            logger.error("File-logging is activated, filename is <result_id>.log, you must also specify 'result_id'")
            raise Exception("File-logging is activated, filename is <result_id>.log, you must also specify 'result_id'")
        if arguments.connection_type != 'none':
            if arguments.mode in ['r', 'rw']:
                arguments.time_upload = format_tu(arguments.time_upload)
                arguments.device_id = format_di(arguments.device_id)
                arguments.data_source_id = format_dsi(arguments.data_source_id)
                check_reading_lengths(arguments.time_upload, arguments.device_id, arguments.data_source_id)
            if arguments.mode in ['w', 'rw']:
                arguments.result_id = format_ri(arguments.result_id)
        check_a(arguments.analysis)
        arguments.analysis_args = format_aa(arguments.analysis_args)
    except Exception as err:
        logger.error("Parsed arguments check failed: " + str(err))
        raise Exception("Parsed arguments check failed: " + str(err))
    logger.debug("Parsed arguments successfully checked")
    return arguments


def format_tu(time_upload):
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
                d_min = utils.string_to_date(time_upload[i])
                d_max = utils.string_to_date(time_upload[i + 1])
                output.append((d_min, d_max))
        except Exception as err:
            raise Exception("Impossible to convert to datetime: " + str(err))
    else:
        raise Exception("'time_upload' length must be even number, current length = " + str(len(time_upload)))
    logger.debug("Modified 'time_upload': " + str(output))
    return output


def format_di(device_id):
    """
    Checks and reformats device id argument.

    :param device_id: UUID https://en.wikipedia.org/wiki/Universally_unique_identifier
    :return: list of uuid objects
    """
    output = []
    logger.debug("Checking and reformatting 'device_id': " + str(device_id))
    if device_id is None:
        raise Exception("No 'device_id' provided for 'r' or 'rw' mode")
    try:
        for i in range(len(device_id)):
            output.append(uuid.UUID(device_id[i]))
    except Exception as err:
        raise Exception("Impossible to convert to UUID : " + str(err))
    logger.debug("Checked 'device_id': " + str(output))
    return output


def format_dsi(data_source_id):
    """
    Checks and reformats data source id argument.

    :param data_source_id: number of source
    :return: list of integers
    """
    output = []
    logger.debug("Checking and reformatting 'data_source_id': " + str(data_source_id))
    if data_source_id is None:
        raise Exception("No 'data_source_id' provided for 'r' or 'rw' mode")
    try:
        for i in range(len(data_source_id)):
            output.append(int(data_source_id[i]))
    except Exception as err:
        raise Exception("Impossible to convert to integer : " + str(err))
    logger.debug("Checked 'data_source_id' : " + str(output))
    return output


def check_reading_lengths(time_upload, device_id, data_source_id):
    """
    Checks if lengths of reading parameters equal

    :param time_upload: list of tuples of datetimes [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
    :param device_id: list of uuid objects [uuid1, uuid2, ..., uuidN]
    :param data_source_id: list of integers [id1, id2, ..., idN]
    """
    lengths = [len(time_upload), len(device_id), len(data_source_id)]
    logger.debug("Lengths of 'time_upload', 'device_id', 'data_source_id': " + str(lengths))
    if len(device_id) != len(data_source_id) or len(device_id) != len(time_upload):
        logger.error("'time_upload', 'device_id', 'data_source_id' have different lengths: " + str(lengths))
        raise Exception("'time_upload', 'device_id', 'data_source_id' have different lengths: " + str(lengths))


def format_ri(result_id):
    """
    Checks and reformats result id argument.

    :param result_id: UUID https://en.wikipedia.org/wiki/Universally_unique_identifier
    :return: uuid object
    """
    output = []
    logger.debug("Checking and reformatting 'result_id': " + str(result_id))
    if result_id is None:
        raise Exception("No 'result_id' provided for 'w' or 'rw' mode")
    try:
        for i in range(len(result_id)):
            output.append(uuid.UUID(result_id[i]))
    except Exception as err:
        raise Exception("Impossible to convert to UUID : " + str(err))
    logger.debug("Checked 'result_id': " + str(output))
    return output


def check_a(analysis):
    """
    Checks if analysis function in analytics.ANALYSIS list

    :param analysis: function name
    """
    logger.debug("Checking analysis existence: " + str(analysis))
    if not analytics.check_analysis(analysis):
        logger.error(
            "Analysis function '" + str(analysis) + "' not found, available functions: " + str(analytics.ANALYSIS))
        raise Exception(
            "Analysis function '" + str(analysis) + "' not found, available functions: " + str(analytics.ANALYSIS))
    logger.debug("Analysis exists: " + str(analysis))


def format_aa(analysis_args):
    """
    Checks and converts to dictionary analysis args argument.

    :param analysis_args: list of key-value pairs [<k1 n1 v11 v12 v13 k2 n2 v21 v22 v23 ... kN nN vN1 vN2 vN3>']
    :return: dictionary {'k1': [v11, v12, v13], 'k2': [v11, v12, v13], ..., 'kN': [vN1, vN2, vN3]}
    """
    output = {}
    logger.debug("Checking and reformatting 'analysis_args': " + str(analysis_args))

    if analysis_args is not None:
        next_count = 0
        for i in range(len(analysis_args)):
            if next_count == i:
                key = analysis_args[i]
                count = int(analysis_args[i + 1])
                temp = analysis_args[i + 2:i + 2 + count]
                output[key] = temp
                next_count = i + 2 + count
                i = next_count - 1

    logger.debug("Modified 'analysis_args': " + str(output))
    return output


def data_to_df(data):
    """
    Converts queries results to dataframe

    :param data:
    """
    logger.debug("Converting downloaded data to DataFrame")
    df = pd.DataFrame()
    # create DataFrame from DB data
    try:
        if len(data) > 0:
            for i in range(len(data)):
                dat = data[i]
                if len(dat) > 0:
                    column = str(dat[0]['device_id']) + '_' + str(dat[0]['data_source_id'])
                    temp = pd.DataFrame.from_dict(dat)
                    temp.rename({'value': column}, axis=1, inplace=True)
                    temp.drop(['device_id', 'data_source_id'], axis=1, inplace=True)
                    if i > 0:
                        df = pd.merge(df, temp, how='outer', left_on='time_upload', right_on='time_upload')
                    else:
                        df = temp
            # Sort by date and set time_upload as index
            df.set_index('time_upload', inplace=True)
            df.sort_index(inplace=True)
        else:
            logger.error("No data to convert, 'data' length = 0")
            raise Exception("No data to convert, 'data' length = 0")
    except Exception as err:
        logger.error("Failed to convert data to DataFrame: " + str(err))
        raise Exception("Failed to convert data to DataFrame: " + str(err))
    logger.debug("Downloaded data successfully converted to DataFrame")
    return df


async def cassandra(arg):
    """
    Read, analysis, write routine for Cassandra

    :param arg: parsed arguments
    """
    logger.info("Cassandra routine")
    # Connect to DB and read data
    db_connection = db.InsyteCassandraIO()
    await db_connection.connect(contact_points=arg.host_address, keyspace=arg.database, port=arg.port,
                                username=arg.username, password=arg.password)
    # read if needed
    if arg.mode in ['r', 'rw']:
        data = await db_connection.read_data(device_id=arg.device_id, data_source_id=arg.data_source_id,
                                             time_upload=arg.time_upload, limit=arg.limit)
        df = await data_to_df(data)
    else:
        df = None

    # Analyze data
    output_data = analytics.analyze_cassandra(arg.analysis, arg.analysis_args, df)

    # write if needed
    if arg.mode in ['w', 'rw']:
        _ = await db_connection.write_data(result_id=arg.result_id, output_data=output_data)

    await db_connection.disconnect()


async def influx(arg):
    """
    Read, analysis, write routine for InfluxDB

    :param arg: parsed arguments
    """
    logger.info("InfluxDB routine")

    influx = db.InsyteInfluxIO()
    await influx.connect(arg.host_address, arg.port, arg.username, arg.password, arg.database)

    # read if needed
    if arg.mode in ['r', 'rw']:
        df = await influx.read_data(device_id=arg.device_id, data_source_id=arg.data_source_id,
                                    time_upload=arg.time_upload, limit=arg.limit)
    else:
        df = None

    # Analyze data
    output_data = analytics.analyze_influx(arg.analysis, arg.analysis_args, df)

    # write if needed
    if arg.mode in ['w', 'rw']:
        _ = await influx.write_data(result_id=arg.result_id, output_data=output_data)


async def none(arg):
    """
    analysis routine without database

    :param arg: parsed arguments
    """
    logger.info("No database routine")

    # Analyze data
    output_data = analytics.analyze_none(arg.analysis, arg.analysis_args)


async def main(arg):
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
        print("ERROR")
        logger.error("Session ended with error: " + str(err))


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    asyncio.run(main(args))

"""
--log
--log-path
logs
--log-level
20
--connection-type
influxdb
--host-address
ems.insyte.ru
--database
ems
--port
8086
--username
ems_user
--password
4rERYTPhfTtvU!99
--mode
rw
--result-id
00000000-0000-0000-0000-000000000011
00000000-0000-0000-0000-000000000010
--device-id
c98fda23-9298-4521-af43-64eb46faf13b
c98fda23-9298-4521-af43-64eb46faf13b
--data-source-id
160
161
--time_upload
2018-11-01_00:00:00+0000
2019-02-01_00:00:00+0000
2018-11-01_00:00:00+0000
2019-02-01_00:00:00+0000
--limit
1000
--analysis
test
--analysis-args
operation
add
value
150.0
"""
