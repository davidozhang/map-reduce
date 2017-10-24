#!/usr/bin/python
import re

from map_reduce import MapReduce

class WordCounter(MapReduce):

    def mapper(self, line):
        words = re.compile(r"[\w']+").findall(line)
        for word in words:
            yield (word, 1)

    def reducer(self, key, entries):
        return key, sum(entries)

if __name__ == '__main__':
    lines = [
        'if you can dream and not make dreams your master',
        'if you can think and not make thoughts your aim',
        'if you can meet with Triumph and Disaster',
        'and treat those two impostors just the same',
        'if you can bear to hear the truth youve spoken',
        'twisted by knaves to make a trap for fools',
        'or watch the things you gave your life to broken',
        'and stoop and build em up with worn-out tools'
    ]
    wc = WordCounter(lines)
