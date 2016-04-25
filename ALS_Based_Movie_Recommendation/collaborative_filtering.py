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
Computing the model used for Collaborative Filtering using ALS
NOTE: ratings = /PATH_FOR_RATINGS_FILE
      mode_path = /PATH_FOR_SAVING_FILE
	  
Use this file to compute the model. need to do this only once
'''
sc = SparkContext("local", "collaborative_filtering") #initializing sc
sqlContext = SQLContext(sc)

df = sqlContext.read.load("./tables/ratings")

#movieId, rating, timestamp, userid 
num_ratings = df.select("rating").count()
num_movies = df.select("movieId").distinct().count()
num_users = df.select("userId").distinct().count()

ratings = "./ratings.csv" #The path to ratings file. change this according to file location

#Loading the data using SparkContext
data = sc.textFile(ratings)
ratings_data = data.map(lambda l: l.split(','))
ratings = ratings_data.map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

#Building the recommendation model using Alternating Least Squares
rank = 10
numIterations = 5
model = ALS.train(ratings, rank, numIterations)

#Evaluate the model on training data
testdata = ratings.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

#Lets save the model for future use
print("Model Computed. Saving the model...")
#path file for model
model_path = "./Model_Collaborative_Filtering_test_1"
model.save(sc,model_path)

#Command for loading: model = MatrixFactorizationModel.load(sc, model_path)

