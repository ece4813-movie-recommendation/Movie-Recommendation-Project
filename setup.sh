#!/bin/bash

# install pip, numoy, scipy, and imdbpy
apt-get install python-pip python-numpy python-scipy python-imdbpy

# install flask
pip install flask

# install the python recsys library in your local machine. 
git clone https://github.com/ocelma/python-recsys.git
cd python-recsys
python setup.py install
