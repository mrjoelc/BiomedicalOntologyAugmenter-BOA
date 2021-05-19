from flask import Flask, request, render_template
from flask_restful import Resource, Api, reqparse, abort, marshal, fields
from kafka import KafkaProducer
from json import dumps
import csv

# Initialize Flask
app = Flask(__name__)
api = Api(app)

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'), 
)

def extract(searchitem):
    list_match=[]
    csvfile = csv.reader(open('assets/OBIB.csv', "r"), delimiter=",")
    for row in csvfile:
        if searchitem in row[1]:
            list_match.append([row[1],row[0]])
    return list_match


@app.route("/", methods=["GET"])
def getClassLabel():
    repository = request.args.get('repository')
    keywords = request.args.get('keywords')
    listOfMatches = []
    if(keywords=="None"):
        listOfMatches = extract("")
    else:
        listOfMatches = extract(str(keywords))
    return render_template('searchInOntology.html', repository = repository, keywords=keywords, listOfMatches= listOfMatches)

@app.route("/extract", methods=["GET", "POST"])
def sendClassLabel():
    repository = request.args.get('repository')
    classLabel = request.args.get('classLabel')
    print(repository)
    producer.send(repository, value=classLabel)
    return  "You're going to extract info about class id: " + classLabel + " in " + repository

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)