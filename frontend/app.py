from flask import Flask
from flask import request, render_template, jsonify, url_for
import json

app = Flask(__name__)

global data

@app.route("/")
def index():
    global data
    data = {"data": "Empty"}
    return render_template('index.html')

@app.route("/data", methods=['POST'])
def post_data():
    global data
    data = request.get_json()
    return jsonify(data)

@app.route("/data", methods=['GET'])
def get_data():
    global data
    return jsonify(data)

if __name__ == "__main__":
    global data
    data = {"data": "Empty"}
    app.run(debug=True)
