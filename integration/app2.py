from flask import Flask
from flask import jsonify, render_template, request
from flask import Blueprint
from engine import RecommendationSystem
import imdb
import json

main = Flask(__name__)

ia = imdb.IMDb(accessSystem='http')  # fetch from imdb web server

@main.route('/<int:userid>/svd_recomm/<int:movieid>', methods=['GET'])
def get_svd_recomm(userid, movieid):
    r1 = recomsys.svd_recomm(userid, only_unknown=False)
    l1 = []
    for mid1 in r1:
        info = recomsys.get_brief(mid1)
        l1.append(info)

    #return jsonify({'results': l1})
    return json.dumps(l1)

@main.route('/<int:userid>/svd_recomm_unknown/<int:movieid>', methods=['GET'])
def get_svd_recomm_unknown(userid, movieid):
    r2 = recomsys.svd_recomm(userid, only_unknown=True)
    l2 = []
    for mid2 in r2:
        info = recomsys.get_brief(mid2)
        l2.append(info)
    #return jsonify({'results': l2})
    return json.dumps(l2, ensure_ascii=False)

@main.route('/<int:userid>/svd_similar/<int:movieid>', methods=['GET'])
def get_svd_similar(userid, movieid):
    r3 = recomsys.svd_similar(movieid)
    l3 = []
    for mid3 in r3:
        info = recomsys.get_brief(mid3)
        l3.append(info)
    #return jsonify({'results': l3})
    return json.dumps(l3)

@main.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    global recomsys

    recomsys = RecommendationSystem()
    main.run(debug=True)
