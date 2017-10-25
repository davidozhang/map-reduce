#!/usr/bin/python
import re

from map_reduce import MapReduce
from movie_name_mapper import MovieNameMapper
from movie_rating_mapper import MovieRatingMapper
from read import read

class MovieNameRatingReducer(MapReduce):

    def reducer(self, key, entries):
        return key, sum(entries)/len(entries)

if __name__ == '__main__':
    mn = MovieNameMapper(read('movies.txt'))
    mr = MovieRatingMapper(read('ratings.txt'))

    new_mappings = {}
    for key in mn.mappings:
        if key in mr.mappings:
            new_mappings[mn.mappings[key][0]] = mr.mappings[key]

    mnr = MovieNameRatingReducer([], output=True, mappings=new_mappings)
