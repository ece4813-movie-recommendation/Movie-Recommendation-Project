#This is the recommendation algorithm based on the SVD 	
#This code can be run in real time but the model has to be pre-computed

import recsys.algorithm
from recsys.algorithm.factorize import SVD

'''
SVD recommendation only for unknown movies
'''


#Lets make things Verbose
recsys.algorithm.VERBOSE = True
#Loading the computed model
svd = SVD(filename='movielens_small')
svd.load_data(filename='ratings_small.csv', sep=',', format={'col':0, 'row':1, 'value':2, 'ids':int})
svd.create_matrix()
#Loading the movielens file of movies which has a mapping of movies to movie-id
loop = True

while (loop):
    ratings_file = open('ratings_small.csv', 'r+')
    movie_lens = open('movies.csv', 'r+')
    user_found = False
    movie_found = False
    USERID = int(input("Enter user id: "))
    #Check if the user_id exists. Since currently we are using the small database, we need to check each and every field.
    #If using the complete database, just check if the number lies in the range.
    for rating_row in ratings_file:
        rating_item = rating_row.split(',')
        if (int(rating_item[0]) == USERID):
            user_found = True
            break 
    if (movie_found):
        for movie_row in movie_lens:
            row_item = movie_row.split(',')
            if (int(row_item[0]) == movie_id):
                print ("Movie name is: " + row_item[1])
                break
    if ( user_found == False):
        print "User not found. Please enter a valid user ID\n"
        continue
    #if we reach here, we definitely have a valid movie and user id. Continue with svd recommendation.
    #Getting movies similar to the given movie
    #similar_list = svd.similar(movie_id)
    #GEtting recommended movies
    similar_list = svd.recommend(USERID, n=10, only_unknowns=True, is_row=True)
    #Similar list is a list of similar movies. Format of Tuple (movie_id, similarity_value)
    ratings_file = open('ratings_small.csv', 'r+')
    movie_lens = open('movies.csv', 'r+')
    movie_list = []
    #print similar_list
    for movie in similar_list:
        ratings_file = open('ratings_small.csv', 'r+')
        movie_lens = open('movies.csv', 'r+')
        similar_movie_id = int(movie[0])
        #print movie[0]
        for movie_row in movie_lens:
            movie_item = movie_row.split(',')
            if (int(movie_item[0]) == similar_movie_id):
                movie_name = movie_item[1]
                break
        for rating_row in ratings_file:
            rating_item = rating_row.split(',')
	    if (int(rating_item[1]) == similar_movie_id):
	        movie_tuple = {}
		movie_tuple[movie_name] = rating_item[2]
		movie_list.append(movie_tuple)
		break
	            
    print "Movie\t\t\t" + "Rating"
    for movie in movie_list:
        #print str(movie[0]) + "\t\t\t\t" + str(movie[1]) + "\n"
	print movie
        



