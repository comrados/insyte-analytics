from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


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

    # read parameters (arrays, if not None, must have equal lengths)
    device_id = None  # array of ids [uuid1, uuid2, ..., uuidN]
    data_source_id = None  # array of ids [id1, id2, ..., idN]
    time_upload = None  # array of tuples of dates [(d_min1 d_max1), (d_min2 d_max2), ..., (d_minN d_maxN)]
    limit = None  # limits the number of retrieved rows

    # connection
    auth = None
    cluster = None
    session = None

    # empty constructor
    def __init__(self):
        pass

    @classmethod
    def set_connection_parameters(cls, contact_points=None, keyspace_name=None, port=None, username=None, password=None):
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
    def check_connection_parameters(cls):
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
    def set_read_parameters(cls, device_id=None, data_source_id=None, time_upload=None):
        if device_id is not None:
            cls.device_id = device_id
        if data_source_id is not None:
            cls.data_source_id = data_source_id
        if time_upload is not None:
            cls.time_upload = time_upload

    @classmethod
    def heck_read_parameters(cls):
        if cls.device_id is None:
            raise ValueError("Connection parameter 'device_id' not given")
        if cls.data_source_id is None:
            raise ValueError("Connection parameter 'data_source_id' not given")
        if cls.time_upload is None:
            raise ValueError("Connection parameter 'time_upload' not given")

    @classmethod
    def connect(cls):
        try:
            cls.check_connection_parameters()
            cls.auth = PlainTextAuthProvider(username=cls.username, password=cls.password)
            cls.cluster = Cluster(cls.contact_points, auth_provider=cls.auth, port=cls.port)
            cls.session = cls.cluster.connect(cls.keyspace_name)
        except ValueError as err:
            raise ValueError("Impossible to connect: " + repr(err))

    @classmethod
    def read_data(cls):
        cls.session.row_factory = dict_factory

        rows = cls.session.execute('SELECT * FROM data WHERE data_source_id=108 ALLOW FILTERING', timeout=10)


