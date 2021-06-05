from kafka import KafkaConsumer, consumer
from json import loads, dumps
import time
import json
import pymongo


def createConsumer():
    return  KafkaConsumer(
            'reducer',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit='latest',
            group_id='my-group-id',
            value_deserializer=lambda x: loads(x.decode('utf-8')))


def write_result_on_Mongo(single_result):
    db = client.boe_database
    mycol = db.results
    old_score = getScore_if_IRI_exists(single_result['IRI'])

    if old_score > 0: #se esiste uno score corrispondente alla ricerca, allora valuta se è peggiore o migliore di quello attuale
        replace_if_better(old_score, single_result, mycol)
    else:             #se non esiste, aggiungi direttamente
        print("Document not exists: " + single_result['IRI'])    
        mycol.insert_one(single_result)
    
def replace_if_better(old_score, single_result, mycol):
    if single_result['score']>old_score: #se il risultato è migliore del vecchio score, allora cancella il vecchio record e aggiungi il nuovo
        print("New score is better: " + str(single_result['score']))
        x = { "IRI": single_result['IRI'] }
        mycol.delete_one(x)
        mycol.insert_one(single_result)
    else:
        print("Old score is better or equal: " + str(single_result['score'])) #se il vecchio score è migliore o uguale lascia com'è 

    
def getScore_if_IRI_exists(iri): #verifia la presenza dell'IRI nel database
    db = client.boe_database
    mycol = db.results
    myquery = { "IRI": iri }
    mydoc = mycol.find_one(myquery)
    if(mydoc):
     return mydoc['score'] #se l'IRI è presente ritorna score corrispondente
    return -1 # se l'IRI non c'è, ritorna -1

for i in range(0,100):
    try:
        consumer = createConsumer()
        #producer = createProducer()
    except:
        time.sleep(2)
        continue
    print("Consumer & producer created at " + str(i) + " tentative")
    break
    

#definiamo il client per Mongo
client = pymongo.MongoClient(host='localhost', 
                             port=27017,
                             username='root', 
                             password='example')



# with open("all_results.json", "r+") as jsonFile:
#     data = json.load(jsonFile)
#     counter = data['counter']+1


for event in consumer:
    res = loads(event.value)
    write_result_on_Mongo(res)
    
    # with open("all_results.json", "r+") as jsonFile:
    #     data = json.load(jsonFile)
    #     data[str(counter)] =loads(res)
    #     jsonFile.seek(0)
    #     data["counter"] = counter
    #     json.dump(data, jsonFile, indent=4, sort_keys=False)
    #     jsonFile.truncate()
    #     print("added n: " + str(counter))
    #     counter+=1

    


