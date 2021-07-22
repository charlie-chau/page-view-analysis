from time import sleep
from json import dumps
import random
import uuid
import time
from kafka import KafkaProducer

def current_milli_time():
    return round(time.time() * 1000)

NUM_USERS = 5
NUM_PAGES = 5
EVENTS_PER_SECOND = 1

users = []
pages = []

for i in range(NUM_USERS):
    users.append(str(uuid.uuid4()))

for i in range(NUM_PAGES):
    pages.append(str(uuid.uuid4()))

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

while True:
    data = {
        'userId': users[random.randint(0, len(users) - 1)],
        'pageId': pages[random.randint(0, len(pages) - 1)],
        'timestamp': current_milli_time()
    }
    producer.send('view_events', value=data)
    print(dumps(data))
    sleep(1/EVENTS_PER_SECOND)
