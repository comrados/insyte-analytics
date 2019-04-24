from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, BatchStatement

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


class CassandraIO:
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
        pass

    @classmethod
    def _set_connection_parameters(cls, contact_points=None, keyspace_name=None, port=None, username=None,
                                   password=None):
        """set connection parameters"""
        if contact_points is not None:
            cls.contact_points = contact_points
        if keyspace_name is not None:
            cls.keyspace_name = keyspace_name
        if port is not None:
            cls.port = port
        if username is not None:
            cls.username = username
        if password is not None:
            cls.password = password

    @classmethod
    def _check_connection_parameters(cls):
        """check connection parameters"""
        if cls.contact_points is None:
            raise ValueError("Connection parameter 'contact_points' not given")
        if cls.keyspace_name is None:
            raise ValueError("Connection parameter 'keyspace_name' not given")
        if cls.port is None:
            raise ValueError("Connection parameter 'port' not given")
        if cls.username is None:
            raise ValueError("Connection parameter 'username' not given")
        if cls.password is None:
            raise ValueError("Connection parameter 'password' not given")

    @classmethod
    def _set_read_parameters(cls, device_id=None, data_source_id=None, time_upload=None):
        """set reading parameters"""
        if device_id is not None:
            cls.device_id = device_id
        if data_source_id is not None:
            cls.data_source_id = data_source_id
        if time_upload is not None:
            cls.time_upload = time_upload

    @classmethod
    def _check_read_parameters(cls):
        """check reading parameters"""
        if cls.device_id is None:
            raise ValueError("Reading parameter 'device_id' not given")
        if cls.data_source_id is None:
            raise ValueError("Reading parameter 'data_source_id' not given")
        if cls.time_upload is None:
            raise ValueError("Reading parameter 'time_upload' not given")
        if len(cls.device_id) != len(cls.data_source_id) or len(cls.device_id) != len(cls.time_upload):
            raise ValueError("Given reading parameters have different lengths")

    @classmethod
    def _set_write_parameters(cls, result_id=None, output_data=None):
        """set writing parameters"""
        if result_id is not None:
            cls.result_id = result_id
        if output_data is not None:
            cls.output_data = output_data

    @classmethod
    def _check_write_parameters(cls):
        """check writing parameters"""
        if cls.result_id is None:
            raise ValueError("Writing parameter 'result_id' not given")
        if cls.output_data is None:
            raise ValueError("Writing parameter 'output_data' not given")

    @classmethod
    def connect(cls, contact_points=None, keyspace_name=None, port=None, username=None, password=None):
        """set connection"""
        cls._set_connection_parameters(contact_points, keyspace_name, port, username, password)
        try:
            cls._check_connection_parameters()
            cls.auth = PlainTextAuthProvider(username=cls.username, password=cls.password)
            cls.cluster = Cluster(cls.contact_points, auth_provider=cls.auth, port=cls.port)
            cls.session = cls.cluster.connect(cls.keyspace_name)
        except ValueError as err:
            raise ValueError("Impossible to connect: " + repr(err))

    @classmethod
    def disconnect(cls):
        """close connection"""
        cls.session.shutdown()
        cls.cluster.shutdown()

    device_id = None  # array of ids [uuid1, uuid2, ..., uuidN]
    data_source_id = None  # array of ids [id1, id2, ..., idN]
    time_upload = None  # array of tuples of dates [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
    limit = None

    @classmethod
    def read_data(cls, device_id=None, data_source_id=None, time_upload=None, limit=None):
        """read data from db according to object's parameters"""
        results = []
        cls._set_read_parameters(device_id, data_source_id, time_upload)
        if limit is not None:
            limit = "LIMIT " + str(limit)
        else:
            limit = ""
        try:
            cls._check_read_parameters()
            for di, dsi, tu in zip(cls.device_id, cls.data_source_id, cls.time_upload):
                params = {"di": di, "dsi": dsi, "from": tu[0], "to": tu[1], "limit": limit}
                query = "SELECT * FROM data WHERE device_id={di} and data_source_id={dsi} and time_upload >= '{from}' and time_upload < '{to}' {limit} ALLOW FILTERING".format(**params)
                result = cls._read(query)
                results.append(result)
        except ValueError as err:
            raise ValueError("Impossible to read: " + repr(err))
        return results

    @classmethod
    def _read(cls, query):
        """executes query and return rows"""
        results = []
        cls.session.row_factory = dict_factory
        rows = cls.session.execute(query, timeout=10)
        results.extend(rows)
        return results

    @classmethod
    def write_data(cls, result_id=None, output_data=None):
        """write data from this object to db"""
        cls._set_write_parameters(result_id, output_data)
        query = "INSERT INTO data_result (result_id, time_upload, value) VALUES (?, ?, ?) IF NOT EXISTS"
        return cls._write(query, cls.result_id, cls.output_data)

    @classmethod
    def _write(cls, query, result_id, output_data, batch_size_limit=25000):
        """insert data into db via batch statements"""
        res = []
        try:
            cls._check_write_parameters()
            batch = BatchStatement()
            prepared = cls.session.prepare(query)
            batch_size = 0
            # send each 50k values in batches
            for d in output_data:
                batch.add(prepared, (result_id, d[0], str(d[1])))
                batch_size += 1
                if batch_size >= batch_size_limit:
                    res.append(cls.session.execute(batch))
                    batch.clear()
                    batch_size = 0
            # send remaining values
            if batch_size > 0:
                res.append(cls.session.execute(batch))
                batch.clear()
        except ValueError as err:
            raise ValueError("Impossible to write: " + repr(err))
        return res
