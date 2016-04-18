from flask import Flask
from flask import request, render_template, jsonify, url_for
import json
import time


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
    d = request.get_data()
    print d
    data = json.loads(d)
    return jsonify(data)

if __name__ == "__main__":
    global data
    data = { "data": "Empty" }
    app.run(debug=True)
