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

        self.online = False
        self.client = None

        self.connect()
        self.disconnect()

    def connect(self):
        """
        Open connection.
        """
        try:
            self.logger.info("Connecting to DB")

            self.client = DataFrameClient(self.host, self.port, self.username, self.password, self.database, timeout=1)
            self.client.ping()
            self.online = True

            self.logger.info("DB Connection set")
        except Exception as err:
            self.logger.error("DB Connection failed: " + str(err))
            raise Exception("DB Connection failed")

    def disconnect(self):
        """
        Close connection.
        """
        self.logger.debug("Closing DB connection")
        if self.client is not None:
            self.client.close()
        self.logger.debug("DB Connection closed")
