# this is the backend engine for the movie recommendation system
# ECE 4813 Team 5
# Thomas Barnes, Rohit Belapurkar,
# Baishen Huang, Zeeshan Khan,
# Rashmi Mehere, Nishant Shah

import recsys.algorithm
from recsys.algorithm.factorize import SVD
import imdb
import urllib
import itertools
from math import sqrt
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark import SparkConf
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import MatrixFactorizationModel
from pyspark.mllib.recommendation import Rating
#import codecs

recsys.algorithm.VERBOSE = True


#rating_file = ratings_small.csv
#movie_file = movies.csv
#modified_file = modified.csv
#model = movielens_small

def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1])) / nratings)

class RecommendationSystem():
    # To run on your own machine, you need to initialize with your datapath to the frontend folder
    def __init__(self, sc, datapath='/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/frontend/', rating_file='ratings_small.csv', complete_rating_file='ratings.csv', movie_file='movies.csv', detail_file='modified.csv', model='movielens_small'):
        self.sc = sc
        self.start = True
        self.rating_file = datapath+rating_file
        self.complete_rating_file = datapath+complete_rating_file
        self.movie_file = datapath+movie_file
        self.detail_file = datapath+detail_file
        self.integration_folder = datapath
        self.svd = SVD(filename=datapath+model)
        self.svd.load_data(filename=self.rating_file, sep=',', format={'col': 0, 'row': 1, 'value': 2, 'ids': int})
        self.svd.create_matrix()
        self.ia = imdb.IMDb(accessSystem='http')

        # als stuff
        self.sqlContext = SQLContext(self.sc)
        self.movie_data = self.sc.textFile(self.movie_file)
        self.ratings_data = self.sc.textFile(self.complete_rating_file).map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), float(x[2])))
        self.als_model_path = datapath + 'Model_Collaborative_Filtering'
        self.als_model = MatrixFactorizationModel.load(sc, self.als_model_path)
        self.movie_df = self.sqlContext.read.load(datapath+'tables/movies')
        self.detail_df = self.sqlContext.read.load(datapath+'tables/detail')
        self.rating_df = self.sqlContext.read.load(datapath+'tables/ratings')


    # call this function to get all recommendations
    def get_all_recomm(self, userid, moviename):
        movieid = self.get_movie_id(moviename)

        # all recommendation algorithms return a list of movie ids
        recom1 = self.svd_recomm(userid, only_unknown=True)
        recom2 = self.svd_similar(movieid)
        recom3 = self.als_new(userid)

        #get info about the movie based on movie ids
        brief_info1 = self.get_brief_list(recom1)
        brief_info2 = self.get_brief_list(recom2)
        brief_info3 = self.get_brief_list(recom3)

        # print to terminal
        for l1 in brief_info1:
            print l1
        for l2 in brief_info2:
            print l2
        for l3 in brief_info3:
            print l3

        return [brief_info1, brief_info2, brief_info3]

    # get movie id based on movie name input
    def get_movie_id(self, moviename):
        r = self.movie_df.where(self.movie_df['name'].startswith(moviename)).first()

        # return movie id 1 if not found
        if r is None:
            return 1

        return r['movieId']

    # svd recommendation algorithm based on the user's rating history, set only_known to True for unseen movies
    def svd_recomm(self, userid, only_unknown):
        user_found = False
        ratings = open(self.rating_file, 'r')
        for rating_row in ratings:
            rating_item = rating_row.split(',')
            if int(rating_item[0]) == userid:
                user_found = True
                break

        ratings.close()
        if not user_found:
            return None

        # output format: (movieid, similarity value)
        if only_unknown:
            similar_list = self.svd.recommend(userid, n=10, only_unknowns=True, is_row=True)
        else:
            similar_list = self.svd.recommend(userid, n=10, only_unknowns=False, is_row=False)

        movieid_list = self.get_id_list(similar_list)
        return movieid_list

    # svd recommendation algorithm based on similar movie
    def svd_similar(self, movieid):
        movie_found = False
        movies = open(self.movie_file, 'r')
        for movie_row in movies:
            row_item = movie_row.split(',')
            if int(row_item[0]) == movieid:
                movie_found = True
                break

        movies.close()
        if not movie_found:
            return None

        similar_list = self.svd.similar(movieid)
        movieid_list = self.get_id_list(similar_list)
        return movieid_list

    # this ALS recommendation algorithm did not get to present to front end
    # future work is needed to improve this algorithm
    def als_recomm(self, userid):
        user_movie_ratings = [16, 24, 32, 47, 50, 110, 150, 161, 165, 204, 223, 256, 260, 261, 277]
        unrated_movies = self.movie_data.filter(lambda x: x[0] not in user_movie_ratings).map(lambda x: (userid, x[0]))
        recommended_movies_rdd = self.als_model.predictAll(unrated_movies)
        # Now we get a list of predictions for all the movies which user has not seen. We take only the top 10 predictions
        user_recommended_ratings_rdd = recommended_movies_rdd.map(lambda x: (x.product, x.rating))

        movie_ID_with_ratings_RDD = self.ratings_data.map(lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        movie_rating_counts_rdd = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

        user_recommended_movies_ratings_count_rdd = (user_recommended_ratings_rdd.join(movie_rating_counts_rdd)).map(lambda l: (l[0], l[1][0], l[1][1]))
        recommended_movies_list = user_recommended_movies_ratings_count_rdd.filter(lambda l: l[2] >= 20).takeOrdered(20, key=lambda x: -x[1])

        return recommended_movies_list

    # an ALS recommendation algorithm based on user rating history
    def als_new(self, userid):
        recommended_movies = self.als_model.recommendProducts(userid, 10)
        recommended_movie_list = []
        for movie in recommended_movies:
            recommended_movie_list.append(movie[1])

        return recommended_movie_list

    # return a list of movie id
    def get_id_list(self, l):
        movieid_list = []
        for s in l:
            movieid_list.append(s[0])
        return movieid_list

    # this function connects to imdb database to get info (including cover image)
    # did not make it to front end due to performance and latency issue
    # need future work for improvement
    def get_detail(self, movieid, imdb_id):
        m = self.ia.get_movie(str(imdb_id))

        cover = m.get('cover url')
        if cover:
            path = self.integration_folder + "Images/" + str(movieid) + ".jpg"
            urllib.urlretrieve(cover, path)

        return m

    # get a list of movie info given a list of movie ids
    def get_brief_list(self, movieList):
        info_list = []
        for m in movieList:
            info = self.get_brief(m)
            if info['title'] != 'unknown':
                info_list.append(info)
            if len(info_list) == 5:
                break

        return info_list

    # get movie info (title, direction, genres, rating, cast) from our rdd database
    def get_brief(self, movieid):
        info = {}
        info['movieid'] = movieid
        info['title'] = 'unknown'
        info['genres'] = 'unknown'
        info['rating'] = 0
        #info['imdbid'] = 1
        info['director'] = 'unknown'
        info['cast'] = 'unknown'

        m = self.movie_df.where(self.movie_df['movieId'] == movieid).first()
        if m is not None:
            info['title'] = m['name']
            info['genres'] = m['genres']
            if len(info['genres']) > 3:
                info['genres'] = info['genres'][0:3]

        d = self.detail_df.where(self.detail_df['movieId'] == movieid).first()
        if d is not None:
            info['director'] = d['director']
            info['cast'] = d['cast']

        r = self.rating_df.where(self.rating_df['movieId'] == movieid)

        # default rating to be 4.6
        if r.count()==0:
            info['rating'] = 4.6
        else:
            avg = r.map(lambda row:row['rating']).reduce(lambda x, y: x+y)/r.count()
            info['rating'] = avg

        return info

# uncomment out for backend engine testing
"""
if __name__ == '__main__':

    sc = SparkContext("local", "recommendation_system")
    rs = RecommendationSystem(sc)

    l = rs.get_all_recomm(1, 'Toy Story')

    l0 = l[0]
    l1 = l[1]
    l2 = l[2]

    for l in l0:
        print l
    for l in l1:
        print l
    for l in l2:
        print l
    """



