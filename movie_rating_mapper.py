#!/usr/bin/python
from map_reduce import MapReduce
from read import read

class MovieRatingMapper(MapReduce):

    def mapper(self, line):
        user_id, movie_id, rating = line.split(', ')
        yield (movie_id, float(rating))
