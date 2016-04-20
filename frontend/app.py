from flask import Flask
from flask import request, render_template, jsonify, url_for
import json
import os, sys
#sys.path.append(os.path.relpath('../integration'))
from engine import RecommendationSystem
from pyspark import SparkContext, SparkConf

import time


app = Flask(__name__)

conf = SparkConf().setAppName("movie_recommendation_server")
sc = SparkContext(conf=conf, pyFiles=['/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/frontend/engine.py'])

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
    data = json.loads(d)
    info = recomsys.get_all_recomm(1, data['data'])
    return jsonify({'data': info})
    #return jsonify(data)

if __name__ == "__main__":
    global data
    global recomsys

    recomsys = RecommendationSystem(sc)

    data = { "data": "Empty" }
    app.run()
