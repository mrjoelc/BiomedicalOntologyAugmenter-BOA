#Authors:
#   Gioele Cageggi - gcageggi@gmail.com
#   Pietro Andrea Vassallo - pietrovassallo04@gmail.com


from kafka import KafkaConsumer, consumer, KafkaProducer
from json import loads, dumps
import time
import numpy as np
import tfidf_matcher as tm



def createConsumer():
    return  KafkaConsumer(
            'matcher',
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='latest',
            enable_auto_commit='latest',
            group_id='my-group-id',
            value_deserializer=lambda x: loads(x.decode('utf-8')))

def createProducer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
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

#create_lookup formatta article da:
#['title', 'abstract', 'conclusion', ['kw',...,'kw']] -> ['title', 'abstrac', 'conclusion', 'kw', ... ,'kw']
def create_lookup(repository_response):
    lookupList = []
    for article in repository_response:
        lookup = []
        lookup.append(article[0])
        lookup.append(article[2])
        if(not article[3]):  
            lookup.append("")
        for keyword in article[1]:
            lookup.append(keyword)
        lookupList.append(lookup)
    return lookupList

def best_matches(term_label, lookupList):
    best_score = 0
    for article in lookupList:
        #per ogni articolo calcola score di similarità con term_label
        result = tm.matcher([term_label],article, len(article)-1).mean()
        #resultpy = result.astype(float)
        score = np.float64(result).item()
        #manteniamo sempre il miglior score
        if score >= best_score :
          best_score = score
          best_match = article      
    return formatArticle(best_match), best_score

 
#formatArticle formatta article da:
#['title', 'abstrac', 'conclusion', 'kw', ... ,'kw'] -> ['title', 'abstract', 'conclusion', ['kw',...,'kw']]
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
        

        #Mantiene per una data ricerca sia tutti gli articoli per cui si ha match e best match
        # single_result = {
        #     'obib_research_data': obib_research_data,
        #     'matches': matches,
        #     repository : {
        #         "best_match": best_article,
        #         "best_score": best_score,
        #     }
        # }
        # single_result = dumps(single_result)

        print("best-score: " + str(best_score))
        
        #risultati da inoltrare nella pipeline
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

        #ne facciamo il dumps
        result_to_forward = dumps(result_to_forward)
        #print(result_to_forward)

        producer.send("reducer", value=result_to_forward)
    




    




    