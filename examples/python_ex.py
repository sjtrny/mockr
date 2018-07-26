# -*- coding: utf-8 -*-

import re
from mockmr import run_sequence_job

WORD_RE = re.compile(r"[\w']+")

def map_fn(chunk):
    # yield each word in the line
    for word in WORD_RE.findall(chunk):
        yield (word.lower(), 1)

def reduce_fn(key, values):
    yield (key, sum(values))

input_list = ["Hello!", "This is a sample string.", "It is very simple.", "Goodbye!"]

results = run_sequence_job(input_list, map_fn, reduce_fn, n_chunks=None)

print(results)