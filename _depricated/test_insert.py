from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import uuid
import datetime
import math
import matplotlib.pyplot as plt
from cassandra.query import BatchStatement

contact_points = ['92.53.78.60']
keyspace_name = 'ems'
port = 9042
username = 'ems_user'
password = 'All4OnS9daW!'

auth = PlainTextAuthProvider(username=username, password=password)
cluster = Cluster(contact_points, auth_provider=auth, port=port)

session = cluster.connect(keyspace_name)

device_id = uuid.UUID('{00000000-0000-0000-0000-000000000000}')

data_source_id = 108
value = "0"


def f(x):
    return 2*math.cos(x) + 3*math.sin(x*2) + 15*math.sin(x*0.05) + 5*math.sin(x*0.5)


def generator(func, start=datetime.datetime(2015, 1, 1), end=datetime.datetime(2019, 1, 1), delta_min=15):
    dates_data = []
    i = 0
    d = start
    while d < end:
        dates_data.append((d, func(i)))
        d += datetime.timedelta(minutes=delta_min)
        i += 1
    return dates_data


dates_data = generator(f)

x = [d[0] for d in dates_data[:500]]
y = [d[1] for d in dates_data[:500]]

#plt.plot(x, y)
#plt.show()


# send batches of 50k values (max batch size 65535)
def execute_batch():
    query = "INSERT INTO data (device_id, data_source_id, time_upload, value) VALUES (?, ?, ?, ?) IF NOT EXISTS"
    batch = BatchStatement()
    prepared = session.prepare(query)
    batch_size = 0
    # send each 50k values in batches
    for d in dates_data:
        batch.add(prepared, (device_id, data_source_id, d[0], str(d[1])))
        batch_size += 1
        if batch_size >= 25_000:
            res = session.execute(batch)
            print('values sent', str(batch_size))
            batch.clear()
            batch_size = 0
    # send remaining values
    if batch_size > 0:
        res = session.execute(batch)
        print('values sent', str(batch_size))
        batch.clear()
        batch_size = 0


def execute_single():
    for d in dates_data:
        query = "INSERT INTO data (device_id, data_source_id, time_upload, value) VALUES (%s, %s, %s, %s) IF NOT EXISTS"
        session.execute(query, (device_id, data_source_id, d[0], str(d[1])))

#print(len(dates_data))

#print(dates_data[0][0])

execute_batch()
