#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 08:30:48 2018

@author: Stephen Tierney
"""

import re
from mockmr.mockmr import MockStreamingJob

WORD_RE = re.compile(r"[\w']+")

class WordCounter(MockStreamingJob):

    def map_fn(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            print("yield map")
            yield (word.lower(), 1)
        
    def reduce_fn(self, key, values):
        print("yield reduce")
        yield (key, sum(values))

input_file = open("MobyDick.txt", 'r')

#input_str = input_file.read()

input_str = "Hello World\nThis is World"

job = WordCounter()

results = job.run(input_str)

print(results)
