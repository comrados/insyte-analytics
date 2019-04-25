from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import dict_factory, BatchStatement
import logging

"""
TABLE data 
(
   device_id uuid,
   data_source_id int,
   time_upload timestamp,
   value text,
   PRIMARY KEY(device_id, data_source_id, time_upload)
);

TABLE data_result (
   result_id uuid,
   time_upload timestamp,
   value text,
   PRIMARY KEY(result_id, time_upload)
);
"""


class InsyteCassandraIO:

    logger = logging.getLogger('InsyteCassandraIO')

    # connection parameters
    contact_points = None
    keyspace_name = None
    port = None
    username = None
    password = None

    # connection
    auth = None
    cluster = None
    session = None

    # read parameters (arrays, must have equal lengths)
    device_id = None  # array of ids [uuid1, uuid2, ..., uuidN]
    data_source_id = None  # array of ids [id1, id2, ..., idN]
    time_upload = None  # array of tuples of dates [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
    limit = None  # limits the number of retrieved rows

    # write parameters
    result_id = None  # uuid of the result
    output_data = None  # array of tuples [(date1, value1), (date2, value2), ..., (dateN, valueN)]

    def __init__(self):
        """empty constructor"""
        self.logger.debug("Initialization")
        pass

    @classmethod
    def _set_connection_parameters(cls, contact_points=None, keyspace_name=None, port=None, username=None,
                                   password=None):
        """set connection parameters"""
        cls.logger.debug("Setting connection parameters")
        if contact_points is not None:
            cls.contact_points = contact_points
            cls.logger.debug("contact_points = " + str(contact_points))
        if keyspace_name is not None:
            cls.keyspace_name = keyspace_name
            cls.logger.debug("keyspace_name = " + str(keyspace_name))
        if port is not None:
            cls.port = port
            cls.logger.debug("port = " + str(port))
        if username is not None:
            cls.username = username
            cls.logger.debug("username = " + str(username))
        if password is not None:
            cls.password = password
            cls.logger.debug("password = " + str(password))

    @classmethod
    def _check_connection_parameters(cls):
        """check connection parameters"""
        cls.logger.debug("Checking connection parameters")
        if cls.contact_points is None:
            cls.logger.warning("Connection parameter 'contact_points' not set")
            raise Exception("Connection parameter 'contact_points' not set")
        if cls.keyspace_name is None:
            cls.logger.warning("Connection parameter 'keyspace_name' not set")
            raise Exception("Connection parameter 'keyspace_name' not set")
        if cls.port is None:
            cls.logger.warning("Connection parameter 'port' not set")
            raise Exception("Connection parameter 'port' not set")
        if cls.username is None:
            cls.logger.warning("Connection parameter 'username' not set")
            raise Exception("Connection parameter 'username' not set")
        if cls.password is None:
            cls.logger.warning("Connection parameter 'password' not set")
            raise Exception("Connection parameter 'password' not set")
        cls.logger.debug("Connection parameters successfully checked")

    @classmethod
    def _set_read_parameters(cls, device_id=None, data_source_id=None, time_upload=None):
        """set reading parameters"""
        cls.logger.debug("Setting reading parameters")
        if device_id is not None:
            cls.device_id = device_id
            cls.logger.debug("device_id = " + str(device_id))
        if data_source_id is not None:
            cls.data_source_id = data_source_id
            cls.logger.debug("data_source_id = " + str(data_source_id))
        if time_upload is not None:
            cls.time_upload = time_upload
            cls.logger.debug("time_upload = " + str(time_upload))

    @classmethod
    def _check_read_parameters(cls):
        """check reading parameters"""
        cls.logger.debug("Checking reading parameters")
        if cls.device_id is None:
            cls.logger.warning("Reading parameter 'device_id' not set")
            raise Exception("Reading parameter 'device_id' not set")
        if cls.data_source_id is None:
            cls.logger.warning("Reading parameter 'data_source_id' not set")
            raise Exception("Reading parameter 'data_source_id' not set")
        if cls.time_upload is None:
            cls.logger.warning("Reading parameter 'time_upload' not set")
            raise Exception("Reading parameter 'time_upload' not set")
        if len(cls.device_id) != len(cls.data_source_id) or len(cls.device_id) != len(cls.time_upload):
            cls.logger.warning("Reading parameters have different lengths")
            raise Exception("Reading parameters have different lengths")
        cls.logger.debug("Reading parameters successfully checked")

    @classmethod
    def _set_write_parameters(cls, result_id=None, output_data=None):
        """set writing parameters"""
        cls.logger.debug("Setting writing parameters")
        if result_id is not None:
            cls.result_id = result_id
            cls.logger.debug("result_id = " + str(result_id))
        if output_data is not None:
            cls.output_data = output_data
            cls.logger.debug("output_data = " + str(output_data))

    @classmethod
    def _check_write_parameters(cls):
        """check writing parameters"""
        cls.logger.debug("Checking writing parameters")
        if cls.result_id is None:
            cls.logger.warning("Writing parameter 'result_id' not set")
            raise Exception("Writing parameter 'result_id' not set")
        if cls.output_data is None:
            cls.logger.warning("Writing parameter 'output_data' not set")
            raise Exception("Writing parameter 'output_data' not set")
        cls.logger.debug("Writing parameters successfully checked")

    @classmethod
    def connect(cls, contact_points=None, keyspace_name=None, port=None, username=None, password=None):
        """set connection"""
        cls.logger.debug("Setting connection")
        cls._set_connection_parameters(contact_points, keyspace_name, port, username, password)
        try:
            cls._check_connection_parameters()
            cls.auth = PlainTextAuthProvider(username=cls.username, password=cls.password)
            lbp = DCAwareRoundRobinPolicy()
            cls.cluster = Cluster(cls.contact_points, auth_provider=cls.auth, port=cls.port, load_balancing_policy=lbp)
            cls.session = cls.cluster.connect(cls.keyspace_name)
            cls.logger.debug("Connection set")
        except Exception as err:
            cls.logger.error("Connection failed")
            raise Exception("Impossible to connect: " + str(err))

    @classmethod
    def disconnect(cls):
        """close connection"""
        cls.logger.debug("Closing connection")
        if cls.session is not None:
            cls.session.shutdown()
        if cls.cluster is not None:
            cls.cluster.shutdown()
        cls.logger.debug("Connection closed")

    @classmethod
    def read_data(cls, device_id=None, data_source_id=None, time_upload=None, limit=None):
        """read data from db according to object's parameters"""
        cls.logger.debug("Reading data")
        results = []
        cls._set_read_parameters(device_id, data_source_id, time_upload)
        if limit is not None:
            cls.logger.debug("Data reading limit set to " + str(limit))
            limit = "LIMIT " + str(limit)
        else:
            limit = ""
        try:
            cls._check_read_parameters()
            for di, dsi, tu in zip(cls.device_id, cls.data_source_id, cls.time_upload):
                params = {"di": str(di), "dsi": str(dsi), "from": str(tu[0]), "to": str(tu[1]), "limit": limit}
                query = "SELECT * FROM data WHERE device_id={di} ".format(**params)
                query += "and data_source_id={dsi} ".format(**params)
                query += "and time_upload >= '{from}' and time_upload < '{to}' ".format(**params)
                query += "{limit} ALLOW FILTERING".format(**params)
                result = cls._read(query)
                results.append(result)
        except Exception as err:
            cls.logger.error("Reading failed")
            raise Exception("Impossible to read: " + str(err))
        cls.logger.debug("Reading complete: " + str([len(x) for x in results]) + " rows returned")
        return results

    @classmethod
    def _read(cls, query):
        """executes query and return rows"""
        cls.logger.debug("Executing reading query " + str(query))
        results = []
        cls.session.row_factory = dict_factory
        rows = cls.session.execute(query, timeout=10)
        results.extend(rows)
        cls.logger.debug("Query executed: " + str(len(results)) + " rows returned")
        return results

    @classmethod
    def write_data(cls, result_id=None, output_data=None, batch_size_limit=25000):
        """write data from this object to db"""
        cls.logger.debug("Writing data")
        cls._set_write_parameters(result_id, output_data)
        query = "INSERT INTO data_result (result_id, time_upload, value) VALUES (?, ?, ?) IF NOT EXISTS"
        result = cls._write(query, batch_size_limit)
        cls.logger.debug("Writing complete " + str(result))
        return result

    @classmethod
    def _write(cls, query, batch_size_limit):
        """insert data into db via batch statements"""
        cls.logger.debug("Writing data in batches of maximum size " + str(batch_size_limit))
        res = []
        try:
            cls._check_write_parameters()
            batch = BatchStatement()
            prepared = cls.session.prepare(query)
            batch_size = 0
            # send each 50k values in batches
            for d in cls.output_data:
                batch.add(prepared, (cls.result_id, d[0], str(d[1])))
                batch_size += 1
                if batch_size >= batch_size_limit:
                    cls.logger.debug("Writing batch " + str(batch_size))
                    res.append(cls.session.execute(batch))
                    batch.clear()
                    batch_size = 0
            # send remaining values
            if batch_size > 0:
                cls.logger.debug("Writing batch " + str(batch_size))
                res.append(cls.session.execute(batch))
                batch.clear()
        except Exception as err:
            raise Exception("Impossible to write: " + str(err))
        return res
