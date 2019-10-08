import logging
from influxdb import DataFrameClient
import datetime
import pandas as pd
import numpy as np


class InsyteInfluxIO:
    logger = logging.getLogger('insyte_analytics.db.insyte_influx_io')

    def __init__(self):
        """
        Constructor.
        """
        self.logger.debug("Initialization")
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.database = None
        self.client = None  # db connection instance

        self.device_id = None
        self.data_source_id = None
        self.time_upload = None

    def _set_connection_parameters(self, host=None, database=None, port=None, username=None,
                                   password=None):
        """
        Set connection parameters.
        """
        self.logger.debug("Setting connection parameters")

        self.host = host
        self.logger.debug("host = " + str(host))

        self.database = database
        self.logger.debug("database = " + str(database))

        self.port = port
        self.logger.debug("port = " + str(port))

        self.username = username
        self.logger.debug("username = " + str(username))

        self.password = password
        self.logger.debug("password = " + str(password))

    def _check_connection_parameters(self):
        """
        Check connection parameters.
        """
        self.logger.debug("Checking connection parameters")

        if self.host is None:
            self.logger.warning("Connection parameter 'host' not set")
            raise Exception("Connection parameter 'host' not set")

        if self.database is None:
            self.logger.warning("Connection parameter 'database' not set")
            raise Exception("Connection parameter 'database' not set")

        if self.port is None:
            self.logger.warning("Connection parameter 'port' not set")
            raise Exception("Connection parameter 'port' not set")

        if self.username is None:
            self.logger.warning("Connection parameter 'username' not set")
            raise Exception("Connection parameter 'username' not set")

        if self.password is None:
            self.logger.warning("Connection parameter 'password' not set")
            raise Exception("Connection parameter 'password' not set")

        self.logger.debug("Connection parameters successfully checked")

    async def connect(self, host: str, port: int, username: str, password: str, database: str):
        """
        Open connection.
        """
        try:
            self.logger.error("Connecting to DB")

            self._set_connection_parameters(host[0], database, port, username, password)

            self._check_connection_parameters()

            self.client = DataFrameClient(self.host, self.port, self.username, self.password, self.database)
        except Exception as err:
            self.logger.error("Connection failed: " + str(err))
            raise Exception("Connection failed: " + str(err))

    async def disconnect(self):
        """
        Close connection.
        """
        self.logger.debug("Closing connection")
        if self.client is not None:
            self.client.close()
        self.logger.debug("Connection closed")

    def _set_read_parameters(self, device_id=None, data_source_id=None, time_upload=None):
        """
        Set reading parameters.

        :param device_id: list of ids [uuid1, uuid2, ..., uuidN]
        :param data_source_id: list of ids [id1, id2, ..., idN]
        :param time_upload: list of tuples of dates [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
        """
        self.logger.debug("Setting reading parameters")

        self.device_id = device_id
        self.logger.debug("device_id = " + str(device_id))

        self.data_source_id = data_source_id
        self.logger.debug("data_source_id = " + str(data_source_id))

        self.time_upload = time_upload
        self.logger.debug("time_upload = " + str(time_upload))

    def _check_read_parameters(self):
        """
        Check reading parameters.
        """
        self.logger.debug("Checking reading parameters")

        if self.device_id is None:
            self.logger.warning("Reading parameter 'device_id' not set")
            raise Exception("Reading parameter 'device_id' not set")

        if self.data_source_id is None:
            self.logger.warning("Reading parameter 'data_source_id' not set")
            raise Exception("Reading parameter 'data_source_id' not set")

        if self.time_upload is None:
            self.logger.warning("Reading parameter 'time_upload' not set")
            raise Exception("Reading parameter 'time_upload' not set")

        if len(self.device_id) != len(self.data_source_id) or len(self.device_id) != len(self.time_upload):
            self.logger.warning("Reading parameters have different lengths")
            raise Exception("Reading parameters have different lengths")

        self.logger.debug("Reading parameters successfully checked")

    async def read_data(self, device_id=None, data_source_id=None, time_upload=None, limit=None):
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

            self._set_read_parameters(device_id, data_source_id, time_upload)

            if limit is not None:
                self.logger.debug("Data reading limit set to " + str(limit))
                limit = "LIMIT " + str(limit)
            else:
                limit = ""

            self._check_read_parameters()

            for di, dsi, tu in zip(self.device_id, self.data_source_id, self.time_upload):

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

    def _set_write_parameters(self, result_id=None, output_data=None):
        """
        Set writing parameters

        :param result_id: uuid of the result
        :param output_data: DataFrame
        """
        self.logger.debug("Setting writing parameters")
        if result_id is not None:
            self.result_id = result_id
            self.logger.debug("result_id = " + str(result_id))
        if output_data is not None:
            self.output_data = output_data
            self.logger.debug("output_data shape = " + str(output_data.shape))

    def _check_write_parameters(self):
        """
        Check writing parameters
        """
        self.logger.debug("Checking writing parameters")
        if self.result_id is None:
            self.logger.warning("Writing parameter 'result_id' not set")
            raise Exception("Writing parameter 'result_id' not set")

        if self.output_data is None:
            self.logger.warning("Writing parameter 'output_data' not set")
            raise Exception("Writing parameter 'output_data' not set")

        if self.output_data.empty:
            self.logger.warning("'output_data' is empty")
            raise Exception("'output_data' is empty")

        if self.output_data.shape[1] != len(self.result_id):
            self.logger.warning("'output_data' and 'result_id' have different lengths: "
                                + str(self.output_data.shape[1]) + " and " + str(len(self.result_id)))
            raise Exception("'output_data' and 'result_id' have different lengths: "
                            + str(self.output_data.shape[1]) + " and " + str(len(self.result_id)))

        self.logger.debug("Writing parameters successfully checked")

    async def write_data(self, result_id=None, output_data=None):
        """
        Write data from this object to db.

        :param result_id: list of ids [uuid1, uuid2, ..., uuidK]
        :param output_data: DataFrame

        :return: list of result objects
        """
        self.logger.debug("Writing data")
        self._set_write_parameters(result_id, output_data)
        results = []
        try:
            self._check_write_parameters()
            for col, ri in zip(self.output_data.columns, self.result_id):
                df = pd.DataFrame(output_data[col])
                df.rename(columns={col: 'value'}, inplace=True)
                _ = self.client.write_points(df, 'data_result', {'result_id': str(ri)})
                results.append(str(ri))
        except Exception as err:
            self.logger.error("Writing failed: " + str(err))
            raise Exception("Writing failed " + str(err))
        self.logger.debug("Writing complete " + str(results))
        return results
