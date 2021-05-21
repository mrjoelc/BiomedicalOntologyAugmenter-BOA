from kafka import KafkaConsumer, consumer, KafkaProducer
from json import loads, dumps
import time
from pymed import PubMed
import ast

def createConsumer():
    return  KafkaConsumer(
            'PubMed',
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

pubmed = PubMed(tool="Searcher", email="cvunict@gmail.com")
def search_pubs_on_pubmed_by_keyword(keyword):
    search = pubmed.query(keyword, max_results=10)
    for article in search:
        # Print the type of object we've found (can be either PubMedBookArticle or PubMedArticle)
        print(type(article))
        # Print a JSON representation of the object
        print(article.toJSON())

pubmed = PubMed(tool="Searcher", email="cvunict@gmail.com")
def search_pubs_on_pubmed_by_keyword2(keyword):
    articlesInfo = []
    search = pubmed.query(keyword, max_results=2)
    for article in search:
        if(article.title and article.title != "" and
           article.keywords and article.keywords != "" and
           article.abstract and article.abstract != ""):
            articlesInfo.append([article.title,
                                article.keywords,
                                article.abstract,
                                article.conclusions])
    return articlesInfo

for event in consumer:
    #attualmente res contiene [classlabel, IRI, description]
    res = ast.literal_eval(event.value)
    
    print("-----RESEARCHED-CLASS-LABEL: " + res[0])
    research = search_pubs_on_pubmed_by_keyword2(res[0])

    x = {
        "repository": 'PubMed',
        "obib_research_data": res,
        "repository_response": research
    }

    res = dumps(x)

    producer.send("matcher", value=res)

        

    
