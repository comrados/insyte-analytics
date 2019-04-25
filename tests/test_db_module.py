from db.insyte_cassandra_io import InsyteCassandraIO
import logging
import os

if __name__ == '__main__':

    print(str("asdasd"))

    print(os.getcwd())

    logging.basicConfig(filename='log.log',
                                filemode='a',
                                format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                                datefmt='%H:%M:%S',
                                level=10)

    logger = logging.getLogger('test')

    contact_points = ['92.53.78.60']
    keyspace_name = 'ems'
    port = 9042
    username = 'ems_user'
    password = 'All4OnS9daW!'

    db = InsyteCassandraIO()

    logger.info("running")

    try:
        db.connect(contact_points=contact_points, keyspace_name=keyspace_name, port=port, username=username, password=password)
        db.disconnect()
        logging.info("connected to DB")
    except Exception as err:
        logging.error(str(err))
        print(str(err))

    logging.shutdown()
