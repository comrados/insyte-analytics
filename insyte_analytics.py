import argparse
import sys
import logging


def init_args(argv):
    """
    Initializes input arguments for interaction with command prompt

        :param argv: input arguments

        :return: returns namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(description='''Reads data from DB, processes, puts back to DB''',
                                     prog='insyte_analytics.py')
    # Logger
    parser.add_argument('-l', '--log', dest='log', default=False, action='store_true',
                        help='enables logging to the file')
    parser.add_argument('-lf', '--log-file', dest='log_file', default="default.log", help='path to save log file')
    parser.add_argument('-ll', '--log-level', dest='log_level', default=20, type=int, help='logging level')

    # Connection
    parser.add_argument('-cps', '--contact-points', dest='contact_points', nargs='+', required=True,
                        help='contact point addresses')
    parser.add_argument('-ks', '--keyspace', dest='keyspace', required=True, help='keyspace name')
    parser.add_argument('-p', '--port', dest='port', required=True, type=int, help='Port')
    parser.add_argument('-un', '--username', dest='username', required=True, help='username')
    parser.add_argument('-pw', '--password', dest='password', required=True, help='password')

    # Reading
    parser.add_argument('-di', '--device-id', dest='device_id', nargs='+', required=True,
                        help='device UUIDs sequence of length N (uuid1 ... uuidN)')
    parser.add_argument('-dsi', '--data-source-id', dest='data_source_id', nargs='+', required=True,
                        help='data source IDs sequence of length N (id1 ... idN)')
    parser.add_argument('-tu', '--time_upload', dest='time_upload', nargs='+', required=True,
                        help='dates set of length 2N (d_min1 d_max1 ... d_minN d_maxN) in format YYYY-mm-dd_HH:MM:SS')
    parser.add_argument('-lim', '--limit', dest='limit', default=None, type=int,
                        help='limit of retrieved DB entries per query')

    # Writing
    parser.add_argument('-ri', '--result-id', dest='result_id', required=True, help='analysis result UUID')

    # Analysis
    parser.add_argument('-a', '--analysis', dest='analysis', required=True, help='analysis function name')

    # TODO dates reformatting, dates to tuples

    '''
    parser.add_argument('-v', '--verbose', help='Verbose output: parsed args, files, elapsed time, sample rate.',
                        default=False, action='store_true')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-p', '--path', help='Path to the folder with .ogg files.', metavar='<path>')
    group.add_argument('-f', '--files', nargs='+', help='Distinct file(s) to convert.', metavar='<files>')
    parser.add_argument('-s', '--sample_rate', help='Sample rate of the output file.', metavar='<sample_rate>',
                        default=16000, type=int)
    parser.add_argument('-o', '--out_path', help='Output path. Script saves to the input directory if omitted.')
    '''
    try:
        args = parser.parse_args(argv)
    except argparse.ArgumentError:
        parser.print_help()
        sys.exit(2)
    else:
        return args
    finally:
        print()


def init_logger(log, log_file, log_level):
    """
    Initialize logger.

    :param log_file: log file path
    :param log_level: https://docs.python.org/3.7/library/logging.html#logging-levels
    :return: Logger object
    """
    if log:
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s.%(msecs)f %(levelname)s %(module)s.%(funcName)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=log_level)
    else:
        logging.basicConfig(filemode='a',
                            format='%(asctime)s.%(msecs)f %(levelname)s %(module)s.%(funcName)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=log_level)
    return logging.getLogger("insyte_analytics")


def main(arg):
    logger = init_logger(arg.log, arg.log_file, arg.log_level)
    logger.info("Session started")
    print(arg.log, arg.log_file, arg.log_level)
    print(arg.contact_points, args.keyspace, args.port, args.username, args.password)
    print(arg.result_id)
    print(arg.device_id, arg.data_source_id, arg.time_upload, arg.limit)
    print(arg.analysis)
    logger.info("Session ended\n")


if __name__ == "__main__":
    # -l --log-file default.log --log-level 10 --contact-points 92.53.78.60 --keyspace ems --port 9042 --username ems_user --password All4OnS9daW! --result-id 00000000-0000-0000-0000-000000000000 --device-id 00000000-0000-0000-0000-000000000000 --data-source-id 1 --time_upload 2018-01-01_00:00:00 2019-01-01_00:00:00 --limit 100 --analysis test
    # -l -lf default.log -ll 10 -cps 92.53.78.60 -ks ems -p 9042 -un ems_user -pw All4OnS9daW! -ri 00000000-0000-0000-0000-000000000000 -di 00000000-0000-0000-0000-000000000000 -dsi 1 -tu 2018-01-01_00:00:00 2019-01-01_00:00:00 -lim 100 -a test
    args = init_args(sys.argv[1:])
    main(args)
