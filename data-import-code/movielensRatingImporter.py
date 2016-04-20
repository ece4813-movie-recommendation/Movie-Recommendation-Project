import os
import re
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

SPARK_HOME = os.environ['SPARK_HOME']
# Regex used to seperate movie movieId, name, year, and genres
RE = re.compile(r'(?P<userId>\d+),(?P<movieId>\d+),(?P<rating>\d\.\d),(?P<timestamp>\d+)')
sc = SparkContext("local", "MovielensLinkImporter") # Initialize the Spark context
sqlContext = SQLContext(sc) # Initialize the SparkSQL context

# Read in the text file as an RDD
#data = sc.textFile(SPARK_HOME + '/ml-latest-small/ratings.csv')
data = sc.textFile('/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/integration/ratings_small.csv')

header = data.first() # Get the csv header
#data = data.filter(lambda line: line != header) # Filter out the csv header

# Split the CSV file into rows
# Formatter that takes the CSV line and outputs it as a list of datapoints
# Uses a regex with named groups
def formatter(line):
    m = RE.match(line) # Seperates datapoints
    if (m != None):
        m = m.groupdict()
        userId = int(m['userId'])
        movieId = int(m['movieId'])
        rating = float(m['rating'])
        timestamp = m['timestamp']
        return [userId, movieId, rating, timestamp]

data = data.map(formatter)
data = data.filter(lambda line: line != None) # Filter out rows that dont match

# Test to make sure all the data is imported
print data.count()

# Map the data into a Row data object to prepare it for insertion
rows = data.map(lambda r: Row(userId=r[0], movieId=r[1], rating=r[2], timestamp=r[3]))

# Create the schema for movies and register a table for it
schemaRatings = sqlContext.createDataFrame(rows)
schemaRatings.registerTempTable("ratings_small")
schemaRatings.save('/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/integration/tables/ratings_small')
