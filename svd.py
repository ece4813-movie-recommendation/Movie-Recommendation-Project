#This algorithm is called singular value decomposition and is used to compute the model from the ratings.csv file
#This needs to be run only once. The computed model is created as a zip folder. 
# U(Sigma)V^T is the mathematical formula used for computing SVD. using the pyrecsys library to implement the SVD algorithm
#Refer to docs for more details on SVD. 

import recsys.algorithm
from recsys.algorithm.factorize import SVD

'''
SVD Model Computation
'''

#To obtain make the script verbose.
recsys.algorithm.VERBOSE = True

#computing the SVD model
svd = SVD()
#loading the ratings file. Format is used to create the matrix for SVD
svd.load_data(filename='ratings_complete.csv', sep=',' , format={'col':0, 'row':1,  'value':2, 'ids':int})
#Now, lets compute the SVD. Formula is M = U(Sigma)V^T
k = 100
svd.compute(k=k, min_values=10, pre_normalize=None, mean_center=True, post_normalize=True, savefile='movielens_complete')

print("Model Computed and Created")
