from db.insyte_cassandra_io import InsyteCassandraIO
import logging
import os
import uuid
import datetime
import math

if __name__ == '__main__':

    def f(x):
        return 2 * math.cos(x) + 3 * math.sin(x * 2) + 15 * math.sin(x * 0.05) + 5 * math.sin(x * 0.5)


    def generator(func, start=datetime.datetime(2015, 1, 1), end=datetime.datetime(2019, 1, 1), delta_min=15):
        dates_data = []
        i = 0
        d = start
        while d < end:
            dates_data.append((d, func(i)))
            d += datetime.timedelta(minutes=delta_min)
            i += 1
        return dates_data


    os.remove('log.log')

    logging.basicConfig(filename='log.log',
                        filemode='a',
                        format='%(asctime)s.%(msecs)f %(levelname)s %(module)s.%(funcName)s %(name)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=10)

    logger = logging.getLogger('test')

    contact_points = ['92.53.78.60']
    keyspace_name = 'ems'
    port = 9042
    username = 'ems_user'
    password = 'All4OnS9daW!'

    device_id = ["00000000-0000-0000-0000-000000000000"]
    data_source_id = [108]
    date = [("2017-01-01", "2018-01-01")]
    limit = ""

    result_id = uuid.uuid4()
    output_data = generator(f, start=datetime.datetime(2019, 1, 1), end=datetime.datetime(2019, 2, 1))

    db = InsyteCassandraIO()

    logger.info("running")

    try:
        db.connect(contact_points=contact_points, keyspace=keyspace_name, port=port, username=username, password=password)
        data = db.read_data(device_id=device_id, data_source_id=data_source_id, time_upload=date)
        result = db.write_data(result_id=result_id, output_data=output_data)
        db.disconnect()
        # logger.info("connected to DB")
    except Exception as err:
        logging.error(str(err))
        print(str(err))

    logging.shutdown()
