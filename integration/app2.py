from flask import Flask
from flask import Blueprint
from flask import jsonify, render_template, request, redirect, url_for
from flask import Blueprint
from engine import RecommendationSystem
import imdb
import json
import httplib, urllib
import requests
import logging
from pyspark import SparkContext, SparkConf

#logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger(__name__)

main = Flask(__name__)
#main = Blueprint('main', __name__)

ia = imdb.IMDb(accessSystem='http')  # fetch from imdb web server

conf = SparkConf().setAppName("movie_recommendation_server")
sc = SparkContext(conf=conf, pyFiles=['/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/integration/engine.py'])

@main.route('/<int:userid>/recomm_all/<int:movieid>')
def get_recomm_all(userid, movieid):
    info = recomsys.get_all_recomm(userid, movieid)
    return json.dumps(info, ensure_ascii=False)

@main.route('/<int:userid>/svd_recomm/<int:movieid>', methods=['GET'])
def get_svd_recomm(userid, movieid):
    r1 = recomsys.svd_recomm(userid, only_unknown=False)
    l1 = []
    for mid1 in r1:
        info = recomsys.get_brief(mid1)
        l1.append(info)
    """
    url = "http://localhost:5000/data"

    #payload = "{\n    \"data\": \"Whatever\"\n}"
    headers = {
        'content-type': "application/json"
    }

    response = requests.request("POST", url, data=json.dumps({'data': l1}, ensure_ascii=False), headers=headers)

    return response.text
    """
    return json.dumps(l1, ensure_ascii=False)


@main.route('/<int:userid>/svd_recomm_unknown/<int:movieid>', methods=['GET'])
def get_svd_recomm_unknown(userid, movieid):
    r2 = recomsys.svd_recomm(userid, only_unknown=True)
    l2 = []
    for mid2 in r2:
        info = recomsys.get_brief(mid2)
        l2.append(info)

    """
    url = "http://localhost:5000/data"

    # payload = "{\n    \"data\": \"Whatever\"\n}"
    headers = {
        'content-type': "application/json"
    }

    response = requests.request("POST", url, data=json.dumps({'data': l2}, ensure_ascii=False), headers=headers)

    return response.text
    #return jsonify({'results': l2})
    """
    return json.dumps(l2, ensure_ascii=False)


@main.route('/<int:userid>/svd_similar/<int:movieid>', methods=['GET'])
def get_svd_similar(userid, movieid):
    r3 = recomsys.svd_similar(movieid)
    l3 = []
    for mid3 in r3:
        info = recomsys.get_brief(mid3)
        l3.append(info)

    """
    headers = {
        'content-type': "application/json"
    }

    response = requests.request("POST", url, data=json.dumps({'data': l3}, ensure_ascii=False), headers=headers)

    return response.text
    """

    #return jsonify({'results': l3})
    return json.dumps(l3, ensure_ascii=False)

@main.route('/<int:userid>/als_new/<int:movieid>', methods=['GET'])
def get_als_new(userid, movieid):
    r4 = recomsys.als_new(userid)
    l4 = []
    for mid4 in r4:
        info = recomsys.get_brief(mid4)
        l4.append(info)
    return json.dumps(l4, ensure_ascii=False)

@main.route('/')
def index():
    return render_template('index.html')

"""
@main.route('/go_to_result', methods=['POST'])
def redirect_to_result():
    #movieid = requests.form['title']
    #return 'hello'
    return redirect(url_for('get_recomm_all'), userid=1, movieid=1)


def create_app(sc):
    global recomsys
    recomsys = RecommendationSystem(sc)
    app = Flask(__name__)
    app.register_blueprint(main)
    return app
"""


if __name__ == '__main__':
    global recomsys
    recomsys = RecommendationSystem(sc)
    main.run(port=5001)


