from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


contact_points = ['92.53.78.60']
keyspace_name = 'ems'
port = 9042
username = 'ems_user'
password = 'All4OnS9daW!'

auth = PlainTextAuthProvider(username=username, password=password)
cluster = Cluster(contact_points, auth_provider=auth, port=port)

session = cluster.connect(keyspace_name)
session.row_factory = dict_factory

device_id = "00000000-0000-0000-0000-000000000000"
data_source_id = 108
date = ("2017-01-01", "2018-01-01")

limit = "LIMIT 10000"

query = "SELECT * FROM data WHERE device_id={} and data_source_id={} and time_upload >= '{}' and time_upload < '{}' {} ALLOW FILTERING".format(device_id, data_source_id, date[0], date[1], limit)

print(query)

rows = session.execute(query, timeout=10)

r = []

r.extend(rows)

print(len(r))
