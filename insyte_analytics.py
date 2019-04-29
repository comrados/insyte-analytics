import argparse
import sys
import logging
import os
import datetime
import uuid
import pandas as pd
import analytics
import db

ANALYSIS = ['test']


def parse_args(argv):
    """
    Parses input arguments from command prompt.

        :param argv: input arguments

        :return: returns namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(description='''Reads data from DB, processes, puts back to DB''',
                                     prog='insyte_analytics.py')
    # Logger
    parser.add_argument('-l', '--log', dest='log', default=False, action='store_true',
                        help='enables logging to the file, filename=<result_id>.log')
    parser.add_argument('-lp', '--log-path', dest='log_path', default="logs", help='log file output folder')
    parser.add_argument('-ll', '--log-level', dest='log_level', default=20, type=int, help='logging level')
    # DB Connection
    parser.add_argument('-cps', '--contact-points', dest='contact_points', nargs='+', required=True,
                        help='contact point addresses')
    parser.add_argument('-ks', '--keyspace', dest='keyspace', required=True, help='keyspace name')
    parser.add_argument('-p', '--port', dest='port', required=True, type=int, help='Port')
    parser.add_argument('-un', '--username', dest='username', required=True, help='username')
    parser.add_argument('-pw', '--password', dest='password', required=True, help='password')
    # DB Reading
    parser.add_argument('-di', '--device-id', dest='device_id', nargs='+', required=True,
                        help='device UUIDs sequence of length N (uuid1 uuid2 ... uuidN)')
    parser.add_argument('-dsi', '--data-source-id', dest='data_source_id', nargs='+', required=True,
                        help='data source IDs sequence of length N (id1 id2 ... idN)')
    parser.add_argument('-tu', '--time_upload', dest='time_upload', nargs='+', required=True,
                        help='dates set of length 2N (d_min1 d_max1 d_min2 d_max2 ... d_minN d_maxN)' +
                             ' in format YYYY-mm-dd_HH:MM:SS±ZZZZ')
    parser.add_argument('-lim', '--limit', dest='limit', default=None, type=int,
                        help='limit of retrieved DB entries per query')
    # DB Writing
    parser.add_argument('-ri', '--result-id', dest='result_id', required=True, help='analysis result UUID')

    # Analysis
    parser.add_argument('-a', '--analysis', dest='analysis', required=True, help='analysis function name')
    parser.add_argument('-aa', '--analysis-args', dest='analysis_args', nargs='*',
                        help='analysis function arguments key-value pairs (key1 val1 key2 val2 ... keyN valN)')
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
        configs['filename'] = os.path.join(log_path, result_id + '.log')
    logging.basicConfig(**configs)
    return logging.getLogger("insyte_analytics")


def check_args(args):
    """
    Checks arguments values and modifies data structures/types.

    :param args: namespace of parsed arguments
    :return: reformatted arguments
    """
    logger.debug("Checking parsed arguments")
    try:
        args.time_upload = format_tu(args.time_upload)
        args.device_id = format_di(args.device_id)
        args.data_source_id = format_dsi(args.data_source_id)
        check_reading_lengths(args.time_upload, args.device_id, args.data_source_id)
        args.result_id = format_ri(args.result_id)
        check_a(args.analysis)
        args.analysis_args = format_aa(args.analysis_args)
    except Exception as err:
        logger.error("Parsed arguments check failed: " + str(err))
        raise Exception("Parsed arguments check failed: " + str(err))
    logger.debug("Parsed arguments successfully checked")
    return args


def format_tu(time_upload):
    """
    Checks and reformats time upload argument.

    :param time_upload: list of upload times (strings) [d_min1, d_max1, d_min2, d_max2, ..., d_minN, d_maxN]
    :return: list of tuples of upload times (datetimes) [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
    """
    output = []
    logger.debug("Checking and reformatting 'time_upload': " + str(time_upload))
    if len(time_upload) % 2 == 0:
        try:
            for i in range(0, len(time_upload), 2):
                d_min = datetime.datetime.strptime(time_upload[i], "%Y-%m-%d_%H:%M:%S%z")
                d_max = datetime.datetime.strptime(time_upload[i + 1], "%Y-%m-%d_%H:%M:%S%z")
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
    logger.debug("Checking and reformatting 'result_id': " + str(result_id))
    try:
        result_id = uuid.UUID(result_id)
    except Exception as err:
        raise Exception("Impossible to convert to UUID : " + str(err))
    logger.debug("Checked 'result_id': " + str(result_id))
    return result_id


def check_a(analysis):
    """
    Checks if analysis function in ANALYSIS list

    :param analysis: function name
    """
    logger.debug("Checking if analysis function '" + analysis + "' exists: " + str(analysis in ANALYSIS))
    if analysis not in ANALYSIS:
        logger.error("Analysis function '" + analysis + "' not found")


def format_aa(analysis_args):
    """
    Checks and converts to dictionary analysis args argument.

    :param analysis_args: list of key-value pairs [key1, val1, key2, val2, ..., keyN, valN]
    :return: dictionary {'key1': val1, 'key2': val2, ..., 'keyN': valN}
    """
    output = {}
    logger.debug("Checking and reformatting 'analysis_args': " + str(analysis_args))
    if len(analysis_args) % 2 == 0:
        for i in range(0, len(analysis_args), 2):
            output[analysis_args[i]] = analysis_args[i + 1]
    else:
        raise Exception("'analysis_args' length must be even number, current length = " + str(len(analysis_args)))
    logger.debug("Modified 'analysis_args': " + str(output))
    return output


def data_to_df(data):
    """
    Converts queries results to dataframe

    :param data:
    :return:
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
            # Sort by date
            df.set_index('time_upload', inplace=True)
            df.sort_index(inplace=True)
            # Fill NaNs
            df.fillna(0., inplace=True)
            df = df.apply(pd.to_numeric)
        else:
            logger.error("No data to convert, 'data' length = 0")
            raise Exception("No data to convert, 'data' length = 0")
    except Exception as err:
        logger.error("Failed to convert data to DataFrame: " + str(err))
        raise Exception("Failed to convert data to DataFrame: " + str(err))
    logger.debug("Downloaded data successfully converted to DataFrame")
    return df


def analyze(analysis, arguments, data_frame):
    """
    Calls analysis functions, returns analysis result.

    :param analysis: analysis function name
    :param arguments: dictonary of analysis function arguments
    :param data_frame: dataframe with data (time series)
    :return: list of tuples for writing [(date1, value1), (date2, value2), ..., (dateN, valueN)]
    """
    logger.debug("Starting analysis")
    try:
        if analysis == 'test':
            result = analytics.TestAnalysis().analyze(arguments, data_frame)
        else:
            logger.error("Analysis function doesn't exist: " + analysis)
            raise Exception("Analysis function doesn't exist: " + analysis)
    except Exception as err:
        logger.error("Analysis failed: " + str(err))
        raise Exception("Analysis failed: " + str(err))
    logger.debug("Analysis successfully complete")
    result.reset_index(inplace=True)
    return [tuple(x.values()) for x in result.to_dict('records')]


def main(arg):
    global logger
    logger = init_logger(arg.log, arg.log_path, arg.log_level, arg.result_id)
    logger.info("Session started")
    try:
        # Check and modify args
        check_args(arg)

        # Connect to DB and read data
        db_connection = db.InsyteCassandraIO()
        db_connection.connect(contact_points=arg.contact_points, keyspace=arg.keyspace, port=arg.port,
                              username=arg.username, password=arg.password)
        data = db_connection.read_data(device_id=arg.device_id, data_source_id=arg.data_source_id,
                                       time_upload=arg.time_upload)
        df = data_to_df(data)
        # Analyze data, write back and disconnect
        output_data = analyze(arg.analysis, arg.analysis_args, df)
        # TODO improve output (list output)
        # result = db.write_data(result_id=arg.result_id, output_data=output_data)
        db_connection.disconnect()
        logger.info("Session successfully ended")
    except Exception as err:
        logger.error("Session ended with error: " + str(err))
    print(arg.log, arg.log_path, arg.log_level)
    print(arg.contact_points, args.keyspace, args.port, args.username, args.password)
    print(arg.result_id)
    print(arg.device_id, arg.data_source_id, arg.time_upload, arg.limit)
    print(arg.analysis, arg.analysis_args)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    main(args)

    '''
    -l
    -lp
    logs
    -ll
    10
    -cps
    92.53.78.60
    -ks
    ems
    -p
    9042
    -un
    ems_user
    -pw
    All4OnS9daW!
    -ri
    00000000-0000-0000-0000-000000000000
    -di
    00000000-0000-0000-0000-000000000000
    00000000-0000-0000-0000-000000000000
    -dsi
    108
    107
    -tu
    2017-02-01_00:00:00+0000
    2018-02-01_00:00:00+0000
    2017-01-01_00:00:00+0000
    2018-01-01_00:00:00+0000
    -lim
    100
    -a
    test
    -aa
    operation
    add
    value
    150.0
    '''

    '''
    --log
    --log-path
    logs
    --log-level
    10
    --contact-points
    92.53.78.60
    --keyspace
    ems
    --port
    9042
    --username
    ems_user
    --password
    All4OnS9daW!
    --result-id
    00000000-0000-0000-0000-000000000000
    --device-id
    00000000-0000-0000-0000-000000000000
    00000000-0000-0000-0000-000000000001
    --data-source-id
    1
    2
    --time_upload
    2018-01-01_00:00:00+0000
    2019-01-01_00:00:00+0000
    2018-01-01_00:00:00+0000
    2019-01-01_00:00:00+0000
    --limit
    100
    --analysis
    test
    --analysis-args
    key1
    val1
    key2
    val2
    '''
