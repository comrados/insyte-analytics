from influxdb import DataFrameClient

host = "ems.insyte.ru"
port = 8086

user = "ems_user"
password = "4rERYTPhfTtvU!99"
dbname = 'ems'

client = DataFrameClient(host, port, user, password, dbname)

res = client.query("select * from data_result where result_id = '00000000-0000-0000-0000-000000000014'")

print(res)
