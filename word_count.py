#!/usr/bin/python
import re

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
    wc = WordCounter(read('word_count.txt'), output=True)
