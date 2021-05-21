from kafka import KafkaConsumer, consumer, KafkaProducer
from json import loads, dumps
import time
import numpy as np
import tfidf_matcher as tm



def createConsumer():
    return  KafkaConsumer(
            'matcher',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit='latest',
            group_id='my-group-id',
            value_deserializer=lambda x: loads(x.decode('utf-8')))

def createProducer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8'), 
    )


for i in range(0,100):
    try:
        consumer = createConsumer()
       #producer = createProducer()
    except:
        time.sleep(2)
        continue
    print("Consumer & producer created at " + str(i) + " tentative")
    break

def create_lookup(repository_response):
    matches = []
    for article in repository_response:
        luckupList = []
        luckupList.append(article[0])
        for kw in article[1]:
            luckupList.append(kw)
        luckupList.append(article[2])
        if(article[3]):  
            luckupList.append(article[3])
        matches.append(luckupList)
    return matches

def best_matches(original, lookup):
    tmp_mean = 0
    for article in matches:
        result = tm.matcher([original],article, len(article)-1).mean()
        resultpy = np.float64(result).item()
        if resultpy >= tmp_mean :
          tmp_mean = resultpy
          best_match = article
    return best_match, tmp_mean


for event in consumer:
    res = loads(event.value)

    repository = res['repository']
    obib_research_data = res['obib_research_data']
    repository_response = res['repository_response']

    matches = create_lookup(repository_response)
    print("Matches for '" + obib_research_data[0] + "' -> " + str(len(matches)))

    best_article, best_score = best_matches(obib_research_data[0], matches)
    
    print(best_article[0])
    print(best_score)
        
    x = {
        repository : {
            "class_label": obib_research_data[0],
            "class_IRI": obib_research_data[1],
            "best_match": best_article,
            "best_score": best_article,
        }
    }

    res = dumps(x)
    print(res)


    




    