# -*- coding: utf-8 -*-

from mockmr.mockmr import run_pandas_job
import pandas as pd

def map_fn(chunk):
    subset_mean = chunk['Age'].mean()
    yield ("mean", subset_mean)

def reduce_fn(key, values):
    list_values = list(values)
    yield (key, sum(list_values)/len(list_values))

dataframe = pd.read_csv('ages.csv')

results = run_pandas_job(dataframe, map_fn, reduce_fn, n_chunks = 4)

print(results)
