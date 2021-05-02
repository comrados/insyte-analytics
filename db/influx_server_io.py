import logging
from influxdb import DataFrameClient
import datetime
import pandas as pd
import numpy as np


class InfluxServerIO:
    logger = logging.getLogger('influx_server_io')

    def __init__(self, host=None, database=None, port=None, username=None, password=None):
        """
        Constructor.
        """
        self.logger.debug("Setting connection parameters")
        self.host = host
        self.database = database
        self.port = port
        self.username = username
        self.password = password

        self.client = None

    def connect(self):
        """
        Open connection.
        """
        try:
            self.logger.debug("Connecting to DB")

            self.client = DataFrameClient(self.host, self.port, self.username, self.password, self.database, timeout=15)
            self.client.ping()

            self.logger.debug("DB Connection set")
        except Exception as err:
            self.logger.error("DB Connection failed: " + str(err))
            raise Exception("DB Connection failed: " + str(err))

    def disconnect(self):
        """
        Close connection.
        """
        try:
            self.logger.debug("Closing DB connection")
            if self.client is not None:
                self.client.close()
            self.client = None
            self.logger.debug("DB Connection closed")
        except Exception as err:
            self.logger.error("Can't disconnect from DB: " + str(err))
            raise Exception("Can't disconnect from DB: " + str(err))

    def read_data(self, device_id=None, data_source_id=None, time_upload=None, limit=None):
        """
        Read data from db according to object's parameters.
        :param device_id: list of ids [uuid1, uuid2, ..., uuidN]
        :param data_source_id: list of ids [id1, id2, ..., idN]
        :param time_upload: list of tuples of dates [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
        :param limit: retrieved data rows limit
        :return: list of queries results
        """
        results = pd.DataFrame()
        try:
            self.logger.debug("Reading data")

            if limit is not None:
                self.logger.debug("Data reading limit set to " + str(limit))
                limit = "LIMIT " + str(limit)
            else:
                limit = ""

            for di, dsi, tu in zip(device_id, data_source_id, time_upload):

                params = {"di": str(di), "dsi": str(dsi), "limit": limit,
                          "from": datetime.datetime.strftime(tu[0], "%Y-%m-%dT%H:%M:%SZ"),
                          "to": datetime.datetime.strftime(tu[1], "%Y-%m-%dT%H:%M:%SZ")}

                query = r"SELECT value FROM data WHERE device_id='{di}' ".format(**params)
                query += r"and data_source_id='{dsi}' ".format(**params)
                query += r"and time >= '{from}' and time <= '{to}' ".format(**params)
                query += r"{limit}".format(**params)

                self.logger.debug("Executing query " + str(query))

                result = self.client.query(query)
                name = str(di) + '_' + str(dsi)

                if len(result) != 0:
                    r = result['data']
                    self.logger.debug("Column " + name + " contains " + str(len(r)) + " rows")
                    r.rename(columns={"value": name}, inplace=True)
                    results = pd.merge(results, r, how='outer', left_index=True, right_index=True)
                else:
                    self.logger.debug("Column " + name + " contains " + str(0) + " rows")
                    results[name] = np.nan

        except Exception as err:
            self.logger.error("Impossible to read: " + str(err))
            raise Exception("Impossible to read: " + str(err))
        self.logger.debug("Reading complete: " + str(results.shape) + " entries returned")
        return results

    def write_data(self, result_id=None, output_data=None):
        """
        Write data from this object to db.
        :param result_id: list of ids [uuid1, uuid2, ..., uuidK]
        :param output_data: DataFrame
        :return: list of result objects
        """
        self.logger.debug("Writing data")
        results = []
        try:
            for col, ri in zip(output_data.columns, result_id):
                df = pd.DataFrame(output_data[col])
                if col.startswith('bool'):
                    df.rename(columns={col: 'boolean'}, inplace=True)
                elif col.startswith('val'):
                    df.rename(columns={col: 'value'}, inplace=True)
                else:
                    self.logger.warning(
                        "Column name: " + str(col) + " (doesnt's start with 'val' or 'bool', renaming to 'val')")
                    df.rename(columns={col: 'value'}, inplace=True)
                v = self.client.write_points(df, 'data_result', {'result_id': str(ri)})
                results.append(str(ri))
        except Exception as err:
            self.logger.error("Writing to DB failed: " + str(err))
            raise Exception("Writing to DB failed " + str(err))
        self.logger.debug("Writing to DB complete " + str(results))
        return results