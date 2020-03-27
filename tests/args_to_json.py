import argparse
import sys
import logging
import os
import uuid
import pandas as pd
import json


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
    analysis_group.add_argument('-a', '--analysis', dest='analysis', required=True,
                                help='analysis function name')
    analysis_group.add_argument('-aa', '--analysis-args', dest='analysis_args', nargs='*',
                                help='analysis function arguments key-count-value sequences <k1 n1 v11 v12 v13'
                                     'k2 n2 v21 v22 v23 ... kN nN vN1 vN2 vN3>, where kN - Nth key, '
                                     'nN - values count of Nth key, vNi - ith value of Nth key')

    return parser.parse_args(argv)


def check_reading_lengths(time_upload, device_id, data_source_id):
    """
    Checks if lengths of reading parameters equal

    :param time_upload: list of tuples of datetimes [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
    :param device_id: list of uuid objects [uuid1, uuid2, ..., uuidN]
    :param data_source_id: list of integers [id1, id2, ..., idN]
    """
    lengths = [len(time_upload), len(device_id), len(data_source_id)]
    if len(device_id) != len(data_source_id) or len(device_id) != len(time_upload):
        raise Exception("'time_upload', 'device_id', 'data_source_id' have different lengths: " + str(lengths))


def format_aa(analysis_args):
    """
    Checks and converts to dictionary analysis args argument.

    :param analysis_args: list of key-value pairs [<k1 n1 v11 v12 v13 k2 n2 v21 v22 v23 ... kN nN vN1 vN2 vN3>']
    :return: dictionary {'k1': [v11, v12, v13], 'k2': [v11, v12, v13], ..., 'kN': [vN1, vN2, vN3]}
    """
    output = {}

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

    return output


def main(arg):
    arg.analysis_args = format_aa(arg.analysis_args)

    return arg


if __name__ == "__main__":
    ans = [
        "--log --result-id 00000000-0000-0000-0000-000000000010 00000000-0000-0000-0000-000000000011 00000000-0000-0000-0000-000000000001 --device-id c98fda23-9298-4521-af43-64eb46faf13b c98fda23-9298-4521-af43-64eb46faf13b 0d318ce3-d8e6-425d-af0a-2f1e7de7acf5 --data-source-id 160 161 1 --time_upload 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 --limit 300 --analysis test --analysis-args operation 1 add value 1 150.0",
        "--log --result-id 00000000-0000-0000-0000-000000000011 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 --time_upload 2018-11-01_00:00:00+0000 2019-02-01_00:00:00+0000 --analysis demand-response-baseline --analysis-args target_day 1 2018-12-01 exception_days 2 2018-11-05 2018-11-06 except_weekends 1 True",
        "--log --log-path logs --log-level 10 --result-id 00000000-0000-0000-0000-000000000012 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 --time_upload 2018-11-01_00:00:00+0000 2019-02-01_00:00:00+0000 --analysis demand-response-discharge --analysis-args target_day 1 2018-12-01 exception_days 2 2018-11-05 2018-11-06 except_weekends 1 True discharge_start_hour 1 20 discharge_value 1 1.0 discharge_duration 1 4",
        "--log --log-path logs --log-level 10 --result-id 00000000-0000-0000-0000-000000000012 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 --time_upload 2018-11-01_00:00:00+0000 2019-02-01_00:00:00+0000 --analysis demand-response-deviation --analysis-args target_day 1 2018-12-01 exception_days 2 2018-11-05 2018-11-06 except_weekends 1 True discharge_start_hour 1 12 discharge_value 1 1.0 discharge_duration 1 4 mode 1 expected",
        "--log --log-path logs --log-level 10 --result-id 00000000-0000-0000-0000-000000000012 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 --time_upload 2018-11-01_00:00:00+0000 2019-02-01_00:00:00+0000 --analysis demand-response-rrmse --analysis-args target_day 1 2018-12-01 exception_days 2 2018-11-05 2018-11-06 except_weekends 1 True discharge_start_hour 1 12 discharge_value 1 1.0 discharge_duration 1 4 mode 1 expected",
        "--log --log-path logs --log-level 10 --result-id 00000000-0000-0000-0000-000000000012 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 --time_upload 2018-11-01_00:00:00+0000 2019-02-01_00:00:00+0000 --analysis demand-response-boolean --analysis-args target_day 1 2018-12-01 exception_days 2 2018-11-05 2018-11-06 except_weekends 1 True discharge_start_hour 1 12 discharge_value 1 1.0 discharge_duration 1 4 mode 1 expected",
        "--log --log-path logs --log-level 10 --result-id 00000000-0000-0000-0000-000000000012 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 --time_upload 2018-11-01_00:00:00+0000 2019-02-01_00:00:00+0000 --analysis demand-response-check --analysis-args target_day 1 2018-12-01 exception_days 2 2018-11-05 2018-11-06 except_weekends 1 True discharge_start_hour 1 12 discharge_value 1 1.0 discharge_duration 1 4 mode 1 expected",
        "--log --log-path logs --log-level 10 --result-id 00000000-0000-0000-0000-000000000012 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 --time_upload 2018-11-01_00:00:00+0000 2019-02-01_00:00:00+0000 --analysis demand-response-expected --analysis-args target_day 1 2018-12-01 exception_days 2 2018-11-05 2018-11-06 except_weekends 1 True discharge_start_hour 1 12 discharge_value 1 1.0 discharge_duration 1 4",
        "--log --result-id 00000000-0000-0000-0000-000000000013 --mode w --analysis peak-prediction-statistical --analysis-args month 1 2 year 1 2019",
        "--log --mode w --result-id 00000000-0000-0000-0000-000000000014 --analysis peak-prediction-ml --analysis-args model 1 nn date 3 2019-06-26 2019-06-27 2019-06-28 sunrise 3 04:13:00 04:14:00 04:15:00 sunset 3 22:23:00 22:23:00 22:23:00 daylength 3 18:09:00 18:09:00 18:09:00 temperature 3 12.4 13.5375 15.2875 pressure 3 762.4125 759.8125 757.25 humidity 3 74.875 70.25 64.625 windspeed 3 1.25 1.375 3.125",
        "--log --result-id 00000000-0000-0000-0000-000000000005 --device-id c98fda23-9298-4521-af43-64eb46faf13b c98fda23-9298-4521-af43-64eb46faf13b c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 161 159 --time_upload 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 --analysis correlation --analysis-args method 1 spearman",
        "--log --log-level 10 --result-id 00000000-0000-0000-0000-000000000051 00000000-0000-0000-0000-000000000052 00000000-0000-0000-0000-000000000053 --device-id c98fda23-9298-4521-af43-64eb46faf13b c98fda23-9298-4521-af43-64eb46faf13b c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 161 159 --time_upload 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 --analysis normalization --analysis-args min_value 1 -1 max_value 1 1",
        "--log --log-level 10 --result-id 00000000-0000-0000-0000-000000000031 00000000-0000-0000-0000-000000000032 00000000-0000-0000-0000-000000000033 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 160 --time_upload 2018-10-01_00:00:00+0000 2019-04-01_00:00:00+0000 --analysis prediction-holt-winters --analysis-args alpha 1 1.0 beta 1 1.0 gamma 1 1.0 season_length 1 24 n_predictions 1 100",
        "--log --log-level 10 --result-id 00000000-0000-0000-0000-000000000055 --device-id c98fda23-9298-4521-af43-64eb46faf13b --data-source-id 161 --time_upload 2018-10-01_00:00:00+0000 2019-11-01_00:00:00+0000 --analysis autocorrelation --analysis-args step 1 2"
    ]
    for a in ans:
        a = a.split()
        args = parse_args(a)
        out = main(args)
        d = {
            "db_io_parameters": {
                "mode": "rw",
                "result_id": out.result_id,
                "device_id": out.device_id,
                "data_source_id": out.data_source_id,
                "time_upload": out.time_upload,
                "limit": out.limit
            },
            "analysis_parameters": {
                "analysis": out.analysis,
                "analysis_arguments": out.analysis_args
            }
        }

        filename = os.path.join("server_test_jsons", "server_a_" + out.analysis.replace("-", "_") + ".json")
        print(filename)
        j = json.dumps(d)

        with open(filename, "w", encoding='utf-8') as f:
            json.dump(d, f)
