#!/bin/bash

#Use this script to install the python recsys library in your local machine. 

git clone https://github.com/ocelma/python-recsys.git
tar xvzf python-recsys
cd python-recsys
sudo python setup.py install
