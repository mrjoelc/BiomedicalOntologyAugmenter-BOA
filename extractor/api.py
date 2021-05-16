from flask import Flask
from flask_restful import Resource, Api, reqparse, abort, marshal, fields

# Initialize Flask
app = Flask(__name__)
api = Api(app)

@app.route("/")
def hello():
    return "Hello!"

@app.route("/extract/<class_id>")
def extract(class_id):
    print("research word is: " + class_id )
    with open("demo.txt") as openfile:
        for line in openfile:
            if class_id == line.strip():
                return "You're going to extract info about class id: " + class_id
    return "Sorry, i couldn't find the class id you're looking for: " + class_id

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)