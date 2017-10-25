#!/usr/bin/python
import re

from map_reduce import MapReduce
from read import read

class MovieNameMapper(MapReduce):

    def mapper(self, line):
        id, movie = line.split(', ')
        yield (id, movie)
