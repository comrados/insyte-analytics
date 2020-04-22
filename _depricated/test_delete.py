from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

contact_points = ['92.53.78.60']
keyspace_name = 'ems'
port = 9042
username = 'ems_user'
password = 'All4OnS9daW!'

auth = PlainTextAuthProvider(username=username, password=password)
cluster = Cluster(contact_points, auth_provider=auth, port=port)

session = cluster.connect(keyspace_name)

rows = session.execute("DELETE FROM ems.data WHERE device_id=00000000-0000-0000-0000-000000000000 and data_source_id=2 IF EXISTS")

for row in rows:
    print(row)
