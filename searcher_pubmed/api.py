from kafka import KafkaConsumer, consumer, KafkaProducer
from json import loads, dumps
import time
from pymed import PubMed
import ast

def createConsumer():
    return  KafkaConsumer(
            'PubMed',
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


pubmed = PubMed(tool="Searcher", email="cvunict@gmail.com")
def search_pubs_on_pubmed_by_keyword(keyword, limit):
    articlesInfo = []
    search = pubmed.query(keyword, max_results=limit)
    for article in search:
        #pymed.book.PubMedBookArticle 
        print(type(article))
        try: 
            if(article.title and article.title != "" and
               article.keywords and article.keywords != "" and
               article.abstract and article.abstract != ""):
                    print("nnnnnn")
                    articlesInfo.append([article.title,
                                        article.keywords,
                                        article.abstract,
                                        article.conclusions])
        except:
            continue
        
    return articlesInfo

for event in consumer:
    #attualmente res contiene [classlabel, IRI, description]
    res = loads(event.value)
    obib_research_data = ast.literal_eval(res['obib_research_data'])
    limit = res['limit']

    print("-----RESEARCHED-CLASS-LABEL: " + obib_research_data[0])
    research = search_pubs_on_pubmed_by_keyword(obib_research_data[0], int(limit))

    x = {
        "repository": 'PubMed',
        "obib_research_data": obib_research_data,
        "repository_response": research
    }

    res = dumps(x)

    producer.send("matcher", value=res)
