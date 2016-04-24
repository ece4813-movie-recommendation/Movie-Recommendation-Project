# this flask app connects backend engine
# and front end html/css and javascript

from flask import Flask
from flask import request, render_template, jsonify, url_for
import json
from engine import RecommendationSystem
from pyspark import SparkContext, SparkConf

app = Flask(__name__)


# need to change pfFiles to your local directory to point to engine.py
conf = SparkConf().setAppName("movie_recommendation_server")
sc = SparkContext(conf=conf, pyFiles=['/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/frontend/engine.py'])

global data
global userid


@app.route("/")
def index():
    global data
    global userid
    data = {"data": "Empty"}
    userid = 1
    return render_template('index.html')

# change user id through url
@app.route("/<int:user_id>")
def index_id(user_id):
    global data
    global userid
    data = {"data": "Empty"}
    userid = user_id
    return render_template('index.html')

# post movie recommendation results
@app.route("/data", methods=['POST'])
def post_data():
    global data
    global userid
    d = request.get_data()
    data = json.loads(d)

    # calling backend to get all movie recommendations
    info = recomsys.get_all_recomm(userid, data['data'])
    return jsonify({'data': info})

if __name__ == "__main__":
    global data
    global recomsys

    # initialize backend engine
    recomsys = RecommendationSystem(sc)

    data = {"data": "Empty"}
    app.run()
