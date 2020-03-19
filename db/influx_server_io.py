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

            self.client = DataFrameClient(self.host, self.port, self.username, self.password, self.database, timeout=1)
            self.client.ping()

            self.logger.debug("DB Connection set")
        except Exception as err:
            self.logger.error("DB Connection failed: " + str(err))
            raise Exception("DB Connection failed")

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
            raise Exception("Can't disconnect from DB")

    def read_data(self, read_dict):
        pass

