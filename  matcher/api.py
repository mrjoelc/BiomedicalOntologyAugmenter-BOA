from kafka import KafkaConsumer, consumer, KafkaProducer
from json import loads, dumps
import time
import ast


def createConsumer():
    return  KafkaConsumer(
            'matcher',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit='latest',
            group_id='my-group-id',
            value_deserializer=lambda x: loads(x.decode('utf-8')))


for i in range(0,100):
    try:
        consumer = createConsumer()
       #producer = createProducer()
    except:
        time.sleep(2)
        continue
    print("Consumer & producer created at " + str(i) + " tentative")
    break


for event in consumer:
    #res = ast.literal_eval(event.value)
    print("-----RESEARCHED-CLASS-LABEL: " + event.value)
    