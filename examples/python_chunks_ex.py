# -*- coding: utf-8 -*-

import re
from mockmr.mockmr import PythonJob

WORD_RE = re.compile(r"[\w']+")

class WordCounter(PythonJob):

    def map_fn(self, _, sub_list):

        # join all strings in the sub_list
        line = ",".join(sub_list)

        # yield each word in the line
        for word in WORD_RE.findall(line):\
            yield (word.lower(), 1)

    def reduce_fn(self, key, values):
        yield (key, sum(values))

input_list = ["Hello!", "This is a sample string.", "It is very simple.", "Goodbye!"]

job = WordCounter()

results = job.run(input_list, n_chunks=2)

print(results)
