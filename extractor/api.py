from flask import Flask, request, render_template
from flask_restful import Resource, Api, reqparse, abort, marshal, fields
from kafka import KafkaProducer
from json import dumps
import csv, ast

# Initialize Flask
app = Flask(__name__)
api = Api(app)

#creazione producer kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'), 
)

def extract(searchitem): #estrazione informazione da OBIB
    list_match=[]
    csvfile = csv.reader(open('assets/OBIB.csv', "r"), delimiter=",")
    for row in csvfile:
        if searchitem in row[1]:
            list_match.append([row[1],row[0],row[5]]) #si prelevano, term label, IRI, descrizione
    return list_match


@app.route("/", methods=["GET"])
def getClassLabel():
    repository = request.args.get('repository') #seleziona da interfaccia il repository (scholar o pubMed)
    keywords = request.args.get('keywords') #parola chiave di ricerca
    listOfMatches = []
    if(keywords=="None"):  
        listOfMatches = extract("")
    else:
        listOfMatches = extract(str(keywords)) 
    return render_template('searchInOntology.html', repository = repository, keywords=keywords, listOfMatches= listOfMatches)

@app.route("/extract", methods=["GET", "POST"])
def sendClassLabel():
    repository = request.args.get('repository')
    print(repository)
    limit = request.args.get('limit') #seleziona limite di risultati che si vuole ottenere nella su repository
    checkedClassTerms = request.args.getlist('checkedClassTerm')
    s = "You're going to extract info in " + repository + "<br><br>"
    for classTerm in checkedClassTerms:
        #data = ast.literal_eval(classTerm)
        data = {
            'obib_research_data' : classTerm,
            'limit' : limit
        }
        #data.append(str(limit))
        producer.send(repository, value=dumps(data))
        s += classTerm + '<br><br>'
    return s

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)