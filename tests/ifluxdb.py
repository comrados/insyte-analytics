import pandas as pd
from influxdb import DataFrameClient

host = "ems.insyte.ru"
port = 8086

user = "ems_user"
password = "4rERYTPhfTtvU!99"
dbname = 'ems'
protocol = 'json'

client = DataFrameClient(host, port, user, password, dbname)

df = pd.DataFrame(data=list(range(30)), index=pd.date_range(start='2014-11-16', periods=30, freq='H'), columns=['0'])

dbs = client.get_list_database()

print(dbs)

#client.create_database(dbname)
#client.write_points(df, 'demo', protocol=protocol)
#client.write_points(df, 'demo', {'k1': 'v1', 'k2': 'v2'}, protocol=protocol)

result = client.query("select * from data", database=dbname)

print(result)

#client.drop_database(dbname)
