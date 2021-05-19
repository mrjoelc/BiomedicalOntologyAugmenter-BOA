from flask import Flask
from flask_restful import Resource, Api, reqparse, abort, marshal, fields
from kafka import KafkaProducer
from json import dumps

# Initialize Flask
app = Flask(__name__)
api = Api(app)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'), 
)

@app.route("/")
def hello():
    return "Hello!"

@app.route("/extract/<repository>/<class_id>")
def extract(repository, class_id):
    print("research word is: " + class_id )
    with open("demo.txt") as openfile:
        for line in openfile:
            if class_id == line.strip():
                producer.send(repository, value=class_id)
                return "You're going to extract info about class id: " + class_id + " in " + repository
    return "Sorry, i couldn't find the class id you're looking for: " + class_id + " in " + repository

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)