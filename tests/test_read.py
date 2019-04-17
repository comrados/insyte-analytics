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

rows = session.execute('SELECT * FROM data WHERE data_source_id=108 ALLOW FILTERING', timeout=10)

r = []

for row in rows:
    r.append(row)

print(len(r))
