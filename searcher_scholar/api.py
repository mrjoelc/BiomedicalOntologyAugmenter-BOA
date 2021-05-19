from kafka import KafkaConsumer, consumer
from json import loads
from scholarly import scholarly
import time

def createConsumer():
    return  KafkaConsumer(
            'Google-Scholar',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit='latest',
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

def search_pubs_on_scholar_by_keyword(paper_keyword):
#    paper_keyword = "steroid"
    search = scholarly.search_pubs(paper_keyword)
    print("Total Results: " + str(search.total_results))
    for r in search:
        #print(r)
        #dict_fill = next(search)
        #scholarly.pprint(dict_fill)
        bib = r['bib']
        title = bib['title']
        #abstract = bib['abstract']
        print(title)
    #return title, abstract
        
for event in consumer:
    event_data = event.value
    # Do whatever you want
    print("RESEARCHED: " + event_data)
    search_pubs_on_scholar_by_keyword(event_data)
    
