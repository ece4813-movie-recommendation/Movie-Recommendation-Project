import imdb
import csv
import codecs
#import unicodedata

#this code add director and cast info to the links.csv file

ia = imdb.IMDb(accessSystem='http') #fetch from imdb web server

file_name = 'datasets/links.csv'

old = open(file_name, 'rb')
new = codecs.open('modified.csv', 'wb', 'utf-8')
reader = csv.reader(old, delimiter=',')
next(reader)

new.write('movieId,imdbId,tmdId,director,cast\n')

for row in reader:
    id = row[1]
    m = ia.get_movie(id)

    director=''
    cast_list=[]
    cast = []

    if m.get('director'):
        director = m.get('director')[0].get('name')

    if m.get('cast'):
        cast_list = m.get('cast')
        l = len(cast_list)
        if l >= 10:
            cast_list = cast_list[0:9]
        else:
            cast_list = cast_list[0:l]

    cast = [c['name'] for c in cast_list]
    cast_elements = '|'.join(cast)
    line = [row[0], id, row[2], director, cast_elements]
    new.write(','.join(line))
    new.write('\n')
    print id

old.close()
new.close()