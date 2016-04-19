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
    #def __init__(self, datapath='/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/integration/', rating_file='ratings_small.csv', movie_file='movies.csv', detail_file='modified.csv', model='movielens_small'):
    def __init__(self, sc, datapath='/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/integration/', rating_file='ratings_small.csv', complete_rating_file='ratings.csv', movie_file='movies.csv', detail_file='modified.csv', model='movielens_small'):
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


    def get_all_recomm(self, userid, movieid):
        #recom1 = self.svd_recomm(userid, only_unknown=False)
        recom1 = self.svd_recomm(userid, only_unknown=True)
        recom2 = self.svd_similar(movieid)
        recom3 = self.als_new(userid)

        #print type(recom3)

        brief_info1 = self.get_brief_list(recom1)
        brief_info2 = self.get_brief_list(recom2)
        brief_info3 = self.get_brief_list(recom3)

        return [brief_info1, brief_info2, brief_info3]

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

        #output format: (movieid, similarity value)
        if only_unknown:
            similar_list = self.svd.recommend(userid, n=10, only_unknowns=True, is_row=True)
        else:
            similar_list = self.svd.recommend(userid, n=10, only_unknowns=False, is_row=False)

        movieid_list = self.get_id_list(similar_list)
        return movieid_list

    def svd_similar(self, movieid):
        movie_found = False
        movies = open(self.movie_file, 'r')
        for movie_row in movies:
            row_item = movie_row.split(',')
            if (int(row_item[0]) == movieid):
                movie_found = True
                break

        movies.close()
        if not movie_found:
            return None

        similar_list = self.svd.similar(movieid)
        movieid_list = self.get_id_list(similar_list)
        return movieid_list

    def als_recomm(self, userid):
        user_movie_ratings = [16, 24, 32, 47, 50, 110, 150, 161, 165, 204, 223, 256, 260, 261, 277]
        unrated_movies = self.movie_data.filter(lambda x: x[0] not in user_movie_ratings).map(lambda x: (userid, x[0]))
        recommended_movies_rdd = self.als_model.predictAll(unrated_movies)
        # Now we get a list of predictions for all the movies which user has not seen. We take only the top 10 predictions
        user_recommended_ratings_rdd = recommended_movies_rdd.map(lambda x: (x.product, x.rating))
        ##
        movie_ID_with_ratings_RDD = self.ratings_data.map(lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        movie_rating_counts_rdd = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))
        ##
        user_recommended_movies_ratings_count_rdd = (user_recommended_ratings_rdd.join(movie_rating_counts_rdd)).map(lambda l: (l[0], l[1][0], l[1][1]))
        recommended_movies_list = user_recommended_movies_ratings_count_rdd.filter(lambda l: l[2] >= 20).takeOrdered(20, key=lambda x: -x[1])


        print recommended_movies_list
        return recommended_movies_list

    def als_new(self, userid):
        #ratings_data = sc.textFile("./ratings.csv")
        #ratings = ratings_data.map(lambda l: l.split(',')).map(lambda r: (int(r[0]), int(r[1]), float(r[2])))
        recommended_movies = self.als_model.recommendProducts(userid, 10)
        recommended_movie_list = []
        for movie in recommended_movies:
            recommended_movie_list.append(movie[1])

        #print recommended_movie_list
        return recommended_movie_list


    def get_id_list(self, l):
        movieid_list = []
        for s in l:
            movieid_list.append(s[0])
        return movieid_list

    def get_detail(self, movieid, imdb_id):
        #print type(imdb_id)
        m = self.ia.get_movie(str(imdb_id))

        cover = m.get('cover url')
        if cover:
            path = self.integration_folder + "Images/" + str(movieid) + ".jpg"
            urllib.urlretrieve(cover, path)

        return m

    def get_brief_list(self, movieList):
        info_list = []
        for m in movieList:
            info = self.get_brief(m)
            info_list.append(info)
        return info_list

    def get_brief(self, movieid):
        info = {}
        info['movieid'] = movieid
        info['title'] = 'unknown'
        info['genre'] = 'unknown'
        info['rating'] = 0
        info['imdbid'] = 1
        info['director'] = 'unknown'
        info['cast'] = 'unknown'

        movies = open(self.movie_file, 'r')
        for m in movies:
            row_item = m.split(',')
            if int(row_item[0]) == movieid:
                info['title'] = str(row_item[1].strip())
                info['genre'] = str(row_item[2].strip()).split('|')
                break
        movies.close()

        ratings = open(self.rating_file, 'r')
        for r in ratings:
            row_item = r.split(',')
            if int(row_item[1]) == movieid:
                info['rating'] = float(row_item[2].strip())
                break
        ratings.close()

        details = open(self.detail_file, 'r')
        #details = codecs.open(self.detail_file, 'r', 'utf-8')
        for d in details:
            row_item = d.split(',')
            if int(row_item[0]) == movieid:
                #print 'found!'
                info['imdbid'] = int(row_item[1].strip())
                info['director'] = str(row_item[3].strip())
                info['cast'] = str(row_item[4].strip()).split('|')
                break
        details.close()

        return info

if __name__ == '__main__':
    sc = SparkContext("local", "collaborative_filtering_recommendation")
    rs = RecommendationSystem(sc)
    #print type(rs.get_detail('0112556'))

    l = rs.get_all_recomm(1,1)

    l1 = l[0]
    l2 = l[1]
    l3 = l[2]

    for l in l1:
        rs.get_detail(l['movieid'], l['imdbid'])
        print l['imdbid']
    print '-------------'

    for l in l2:
        rs.get_detail(l['movieid'], l['imdbid'])
        print l['imdbid']
    print '-------------'
    for l in l3:
        rs.get_detail(l['movieid'], l['imdbid'])
        print l['imdbid']


    """
    l1 = rs.svd_recomm(1,True)
    l2 = rs.svd_recomm(1, False)
    l3 = rs.svd_similar(1)

    for l in l1:
        m = rs.get_brief(l)
        print m.get('title')

    print '-------'
    for r in l2:
        m = rs.get_brief(r)
        print m.get('title')
    print '-------'
    for k in l3:
        m = rs.get_brief(k)
        # m = rs.get_detail(k)
        print m.get('title')
    """



