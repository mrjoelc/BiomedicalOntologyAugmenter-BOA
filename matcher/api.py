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
        producer = createProducer()
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
        luckupList.append(article[2])
        if(not article[3]):  
            luckupList.append("")
        for kw in article[1]:
            luckupList.append(kw)
        matches.append(luckupList)
    return matches

def best_matches(original, lookup):
    tmp_mean = 0
    for article in lookup:
        result = tm.matcher([original],article, len(article)-1).mean()
        #resultpy = result.astype(float)
        resultpy = np.float64(result).item()
        if resultpy >= tmp_mean :
          tmp_mean = resultpy
          best_match = article
    return formatArticle(best_match), tmp_mean

 #[title, abstrac, "", p, p ,p, p,]
def formatArticle(article):
    keywords = []
    formatted_article = []
    formatted_article.append(article[0])
    formatted_article.append(article[1])
    formatted_article.append(article[2])
    for i in range(3, len(article)-1):
        keywords.append(article[i])
    formatted_article.append(keywords)
    return formatted_article




for event in consumer:
    res = loads(event.value)

    repository = res['repository']
    obib_research_data = res['obib_research_data']
    repository_response = res['repository_response']

    print("Matches for '" + obib_research_data[0] + "' -> " + str(len(repository_response)))

    if(len(repository_response)>0):
        matches = create_lookup(repository_response)
        best_article, best_score = best_matches(obib_research_data[0], matches)
        
        single_result = {
            'obib_research_data': obib_research_data,
            'matches': matches,
            repository : {
                "best_match": best_article,
                "best_score": best_score,
            }
        }

        single_result = dumps(single_result)
        print("best-score: " + str(best_score))

  
        result_to_forward = {
            "IRI" : obib_research_data[1],
            'term_label' : obib_research_data[0],
            'paper' : {
                    'title' : best_article[0],
                    'keywords' : best_article[3],
                    'abstract' : best_article[1],
                    'conclusion' : best_article[2]
                },
            'score': best_score,
        }

        result_to_forward = dumps(result_to_forward)
        #print(result_to_forward)

        producer.send("reducer", value=result_to_forward)
    




    




    