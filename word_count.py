#!/usr/bin/python
import re
import sys

from map_reduce import MapReduce
from read import read

class WordCounter(MapReduce):

    def mapper(self, line):
        words = re.compile(r"[\w']+").findall(line)
        for word in words:
            yield (word, 1)

    def reducer(self, key, entries):
        return key, sum(entries)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception('usage: python word_count.py [file_name]')

    wc = WordCounter(read(sys.argv[1]), output=True)
