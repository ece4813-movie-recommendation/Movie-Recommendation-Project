import recsys.algorithm
from recsys.algorithm.factorize import SVD
import imdb
import urllib
#import codecs

recsys.algorithm.VERBOSE = True


#rating_file = ratings_small.csv
#movie_file = movies.csv
#modified_file = modified.csv
#model = movielens_small

class RecommendationSystem():
    #def __init__(self, spark_context, rating_file='ratings_small.csv', movie_file='movies.csv', detail_file='modified.csv', model='movielens_small'):
    def __init__(self, rating_file='ratings_small.csv', movie_file='movies.csv', detail_file='modified.csv', model='movielens_small'):
        self.start = True
        self.rating_file = rating_file
        self.movie_file = movie_file
        self.detail_file = detail_file
        self.svd = SVD(filename=model)
        self.svd.load_data(filename=rating_file, sep=',', format={'col': 0, 'row': 1, 'value': 2, 'ids': int})
        self.svd.create_matrix()
        self.ia = imdb.IMDb(accessSystem='http')

    def get_all_recomm(self, userid, movieid):
        recom1 = self.svd_recomm(userid, only_unknown=False)
        recom2 = self.svd_recomm(userid, only_unknown=True)
        recom3 = self.svd_similar(movieid)

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

    def get_id_list(self, l):
        movieid_list = []
        for s in l:
            movieid_list.append(s[0])
        return movieid_list

    def get_detail(self, imdb_id):
        #print type(imdb_id)
        m = self.ia.get_movie(str(imdb_id))

        cover = m.get('cover url')
        if cover:
            path = "Images/" + str(imdb_id) + ".jpg"
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
        info['title'] = 'unknown'
        info['genre'] = 'unknown'
        info['rating'] = 0
        info['imdb_id'] = 1
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
                info['imdb_id'] = int(row_item[1].strip())
                info['director'] = str(row_item[3].strip())
                info['cast'] = str(row_item[4].strip()).split('|')
                break
        details.close()

        return info

if __name__ == '__main__':
    rs = RecommendationSystem()
    #print type(rs.get_detail('0112556'))

    l = rs.get_all_recomm(1,1)

    l1 = l[0]
    l2 = l[1]
    l3 = l[2]

    for l in l1:
        print l['title']
    for l in l2:
        print l['title']
    for l in l3:
        print l['title']

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



