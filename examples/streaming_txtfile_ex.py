# -*- coding: utf-8 -*-

import re
from mockmr import run_stream_job

WORD_RE = re.compile(r"[\w']+")

def map_fn(chunk):
    # yield each word in the line
    for word in WORD_RE.findall(chunk):
        yield (word.lower(), 1)

def reduce_fn(key, values):
    yield (key, sum(values))

input_file = open("MobyDick.txt", 'r')

input_str = input_file.read()

results = run_stream_job(input_str, map_fn, reduce_fn)

print(results)
