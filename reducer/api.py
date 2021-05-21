from kafka import KafkaConsumer, consumer
from json import loads, dumps
import time
import json

def createConsumer():
    return  KafkaConsumer(
            'reducer',
            bootstrap_servers=['kafka:9092'],
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

with open("all_results.json", "r+") as jsonFile:
    data = json.load(jsonFile)
    counter = data['counter']+1

for event in consumer:
    res = event.value

    with open("all_results.json", "r+") as jsonFile:
        data = json.load(jsonFile)
        data[str(counter)] =loads(res)
        jsonFile.seek(0)
        data["counter"] = counter
        json.dump(data, jsonFile, indent=4, sort_keys=False)
        jsonFile.truncate()
        print("added n: " + str(counter))
        counter+=1

    


