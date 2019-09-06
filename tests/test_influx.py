import db

influx = db.InsyteInfluxIO()
await influx.connect(r'ems.insyte.ru', 8086, 'ems_user', r"4rERYTPhfTtvU!99", "ems")


df = await influx.read_data(device_id=arg.device_id, data_source_id=arg.data_source_id, time_upload=arg.time_upload, limit=arg.limit)


pass
