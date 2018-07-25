# -*- coding: utf-8 -*-

import re
from mockmr.mockmr import StreamingJob

WORD_RE = re.compile(r"[\w']+")

class WordCounter(StreamingJob):

    def map_fn(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
        
    def reduce_fn(self, key, values):
        yield (key, sum(values))

input_file = open("MobyDick.txt", 'r')

input_str = input_file.read()

job = WordCounter()

results = job.run(input_str)

print(results)
