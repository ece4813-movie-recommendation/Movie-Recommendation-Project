import itertools
from math import sqrt
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark import SparkConf
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import MatrixFactorizationModel
from pyspark.mllib.recommendation import Rating


'''
This is just a demo code. Though this will return some recommendation, this is not what we 
used for the final recommendation engine. THe actual code for recommendation is in engine file
'''


sc = SparkContext("local", "collaborative_filtering_recommendation") #initializing sc
sqlContext = SQLContext(sc)

#Loading the Pre-Computed Model for ALS. 

model_path = "./Model_Collaborative_Filtering" 
movie_path = "./movies.csv"
ratings_path = "./ratings.csv"

movie_data = sc.textFile(movie_path)

ratings_data = sc.textFile(ratings_path).map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), float(x[2])))

model = MatrixFactorizationModel.load(sc, model_path)

'''
numBlocks = number of blocks used to parallelize computation
rank = number of latent factors in the model
iterations = number of iterations to run
lambda = specifies the regularization parameter in ALS
'''

#predicting the rating for any given movie. Might be useful later on or for evaluation
'''
user_id = int(raw_input("Please enter a user ID: "))
movie_id = int(raw_input("Please enter a valid movie ID: "))
'''
def predict_rating(user_id, movie_id):
    user_id = 1
    movie_id = 1

    movie_rating_rdd = sc.parallelize([(user_id, movie_id)])

    predicted_movie_rating = model.predictAll(movie_rating_rdd)

    if (predicted_movie_rating.count() == 0):
        print "Oopsies. Something went wrong. Enter valid user and movie id"
    else:
        return predicted_movie_rating.take(1)
	
def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


#output of the type user,movie,rating. 
def recommend_movies(user_id):
    #Get the users. Here, user_id = 1
    user_id = 1
    '''
    Note: user_movie_ratings is a static list.
    TO DO: Need to make it dynamic based on user_id.
    '''
    user_movie_ratings = [16,24,32,47,50,110,150,161,165,204,223,256,260,261,277]
    unrated_movies = movie_data.filter(lambda x: x[0] not in user_movie_ratings).map(lambda x: (user_id , x[0]))
    recommended_movies_rdd = model.predictAll(unrated_movies)
    #Now we get a list of predictions for all the movies which user has not seen. We take only the top 10 predictions
    user_recommended_ratings_rdd = recommended_movies_rdd.map(lambda x: (x.product, x.rating))
    ##
    movie_ID_with_ratings_RDD = ratings_data.map(lambda x: (x[1], x[2])).groupByKey()
    movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
    movie_rating_counts_rdd = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))    
    ##
    user_recommended_movies_ratings_count_rdd = (user_recommended_ratings_rdd.join(movie_rating_counts_rdd))\
							.map(lambda l: (l[0], l[1][0], l[1][1]))
    recommended_movies_list = user_recommended_movies_ratings_count_rdd.filter(lambda l: l[2]>=20).takeOrdered(20, key=lambda x: -x[1])
    return recommended_movies_list
   
print (recommend_movies(1)) 
