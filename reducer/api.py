from kafka import KafkaConsumer, consumer
from json import loads, dumps
import time
import json
import pymongo


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
    
client = pymongo.MongoClient(host='mongo', 
                             port=27017,
                             username='root', 
                             password='example')


def single_write_mongo(single_result):
    old_score = check_if_IRI_exists(single_result['IRI'])
    eventually_add(old_score, single_result)
    

def eventually_add(old_score, single_result):
    db = client.boe_database
    mycol = db.results
    if old_score>0:
        print("Document exists: " + single_result['IRI'])
        if single_result['score']>old_score:
            print("New score is better: " + str(single_result['score']))
            x = { "IRI": single_result['IRI'] }
            mycol.delete_one(x)
            mycol.insert_one(single_result)
        else:
            print("Old score is better or equal: " + str(single_result['score']))
    else:
        print("Document not exists: " + single_result['IRI'])    
        mycol.insert_one(single_result)

def check_if_IRI_exists(iri):
    db = client.boe_database
    mycol = db.results
    myquery = { "IRI": iri }
    mydoc = mycol.find_one(myquery)
    if(mydoc):
     return mydoc['score']
    return -1

# with open("all_results.json", "r+") as jsonFile:
#     data = json.load(jsonFile)
#     counter = data['counter']+1


for event in consumer:
    res = loads(event.value)
    single_write_mongo(res)
    
    # with open("all_results.json", "r+") as jsonFile:
    #     data = json.load(jsonFile)
    #     data[str(counter)] =loads(res)
    #     jsonFile.seek(0)
    #     data["counter"] = counter
    #     json.dump(data, jsonFile, indent=4, sort_keys=False)
    #     jsonFile.truncate()
    #     print("added n: " + str(counter))
    #     counter+=1

    


