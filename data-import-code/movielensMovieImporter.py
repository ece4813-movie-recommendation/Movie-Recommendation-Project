import os
import re
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

SPARK_HOME = os.environ['SPARK_HOME']
# Regex used to seperate movie movieId, name, year, and genres
RE = re.compile(r'(?P<movieId>\d+),"?(?P<name>.+)\((?P<year>\d+)\) ?"?,(?P<genres>.+)')
sc = SparkContext("local", "MovielensMovieImporter") # Initialize the Spark context
sqlContext = SQLContext(sc) # Initialize the SparkSQL context

# Read in the text file as an RDD
data = sc.textFile('/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/integration/movies.csv')

header = data.first() # Get the csv header
#data = data.filter(lambda line: line != header) # Filter out the csv header

# Split the CSV file into rows
# Formatter that takes the CSV line and outputs it as a list of datapoints
# Uses a regex with named groups
def formatter(line):
    m = RE.match(line) # Seperates datapoints
    if (m != None):
        m = m.groupdict()
        movieId = int(m['movieId'])
        name = m['name']
        year = int(m['year'])
        genres = m['genres'].split('|')
        return [movieId, name, year, genres]

data = data.map(formatter)
data = data.filter(lambda line: line != None) # Filter out rows that dont match

# Test to make sure all the data is imported
print data.count()

# Map the data into a Row data object to prepare it for insertion
rows = data.map(lambda r: Row(movieId=r[0], name=r[1], year=r[2], genres=r[3]))

# Create the schema for movies and register a table for it
schemaMovies = sqlContext.createDataFrame(rows)
schemaMovies.registerTempTable("movies")
schemaMovies.save('/media/psf/Home/CS/GIT_HUB/Movie-Recommendation-Project/integration/tables/movies')
