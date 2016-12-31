# Movie-Recommendation-Project
A recommendation system for movies based on a large dataset obtained from MovieLens and IMDb and make a comparative analysis between Singular Value Decomposition (SVD) and Alternating Least Squares (ALS).

The recommendation system uses Spark MLlib for Machine Learning using the Python API.
The front end is built using a Flask server and HTML/CSS and JavaScript for the interface.
A RESTful JSON API is used to send requests to Spark as well as receive the output from the algorithms.

## Setup
- Download Spark from http://spark.apache.org/downloads.html
- Clone this repository: `git clone https://github.com/ece4813-movie-recommendation/Movie-Recommendation-Project.git`
- `cd` into this directory, install dependencies by typing `./setup.sh`

## Running the code
- Start Spark master and slaves following the instruction from [here](http://spark.apache.org/docs/latest/spark-standalone.html#starting-a-cluster-manually). 
- In the Spark directory, type in `./bin/spark-submit <path_to_this_repository/frontend/app.py>`.
- View and interact with the UI with ip address `localhost:5000` in a browser.

## Screenshots
![UI - Input Prompt](/screenshots/UI-Main.png)
![UI - Output](/screenshots/UI-Recommendations.png)

## Collaborators
This is a collaboration of the following people:
- Thomas Barnes
- Rohit Belapurkar 
- Baishen Huang
- Zeeshan Khan
- Rashmi Mehere
- Nishant Shah 

## Reference
The project is heavily inspired by [this work](https://github.com/jadianes/spark-movie-lens).

