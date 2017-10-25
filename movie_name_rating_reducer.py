#!/usr/bin/python
import sys

from map_reduce import MapReduce
from movie_name_mapper import MovieNameMapper
from movie_rating_mapper import MovieRatingMapper
from read import read

class MovieNameRatingReducer(MapReduce):

    def reducer(self, key, entries):
        return key, sum(entries)/len(entries)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise Exception('usage: python movie_name_rating_reduce.py [movies file] [ratings file]')

    mn = MovieNameMapper(read(sys.argv[1]))
    mr = MovieRatingMapper(read(sys.argv[2]))

    new_mappings = {}
    for key in mn.mappings:
        if key in mr.mappings:
            new_mappings[mn.mappings[key][0]] = mr.mappings[key]

    mnr = MovieNameRatingReducer([], output=True, mappings=new_mappings)
