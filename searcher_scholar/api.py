from kafka import KafkaConsumer, consumer
from json import loads
from scholarly import scholarly
import time
# print(next(scholarly.search_author('Steven A. Cholewiak')))




def createConsumer():
    return  KafkaConsumer(
            'scholar',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group-id',
            value_deserializer=lambda x: loads(x.decode('utf-8')))

for i in range(0,100):
    try:
        consumer = createConsumer()
    except:
        time.sleep(2)
        continue
    print("Consumer created at " + str(i) + " tentative")
    break
        

# consumer = KafkaConsumer(
#     'scholar',
#     bootstrap_servers=['kafka:9092'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-group-id',
#     value_deserializer=lambda x: loads(x.decode('utf-8'))
# )

for event in consumer:
    event_data = event.value
    # Do whatever you want
    print("RESEARCHED: " + event_data)
    search_query = scholarly.search_pubs(event_data)
    scholarly.pprint(next(search_query))
    
