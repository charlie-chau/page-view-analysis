from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

for i in range(10000):
    data = 'Hello World'
    print('Sending {} {}'.format(i, data))
    producer.send('sample', value=data)
    sleep(2)

