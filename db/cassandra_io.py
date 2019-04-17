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

    # connection
    auth = None
    cluster = None
    session = None

    # read parameters (arrays, if not None, must have equal lengths)
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
    def set_connection_parameters(cls, contact_points=None, keyspace_name=None, port=None, username=None, password=None):
        """sets connection parameters"""
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
        """checks connection parameters"""
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
        """sets reading parameters"""
        if device_id is not None:
            cls.device_id = device_id
        if data_source_id is not None:
            cls.data_source_id = data_source_id
        if time_upload is not None:
            cls.time_upload = time_upload

    @classmethod
    def check_read_parameters(cls):
        """checks reading parameters"""
        if cls.device_id is None:
            raise ValueError("Reading parameter 'device_id' not given")
        if cls.data_source_id is None:
            raise ValueError("Reading parameter 'data_source_id' not given")
        if cls.time_upload is None:
            raise ValueError("Reading parameter 'time_upload' not given")

    @classmethod
    def set_write_parameters(cls, result_id=None, output_data=None):
        """sets writing parameters"""
        if result_id is not None:
            cls.result_id = result_id
        if output_data is not None:
            cls.output_data = output_data

    @classmethod
    def check_write_parameters(cls):
        """checks writing parameters"""
        if cls.result_id is None:
            raise ValueError("Writing parameter 'result_id' not given")
        if cls.output_data is None:
            raise ValueError("Writing parameter 'output_data' not given")

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


