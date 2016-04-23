# this flask app do not make it to front end
# it is intended be used for testing integration
# between spark, flask and backend individual function

from flask import Flask
from flask import Blueprint
from flask import jsonify, render_template, request, redirect, url_for
from engine import RecommendationSystem
import imdb
import json
import httplib, urllib
import requests
import logging
from pyspark import SparkContext, SparkConf

main = Flask(__name__)

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
    return json.dumps(l1, ensure_ascii=False)


@main.route('/<int:userid>/svd_recomm_unknown/<int:movieid>', methods=['GET'])
def get_svd_recomm_unknown(userid, movieid):
    r2 = recomsys.svd_recomm(userid, only_unknown=True)
    l2 = []
    for mid2 in r2:
        info = recomsys.get_brief(mid2)
        l2.append(info)
    return json.dumps(l2, ensure_ascii=False)


@main.route('/<int:userid>/svd_similar/<int:movieid>', methods=['GET'])
def get_svd_similar(userid, movieid):
    r3 = recomsys.svd_similar(movieid)
    l3 = []
    for mid3 in r3:
        info = recomsys.get_brief(mid3)
        l3.append(info)
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

if __name__ == '__main__':
    global recomsys
    recomsys = RecommendationSystem(sc)
    main.run(port=5001)


