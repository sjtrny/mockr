# -*- coding: utf-8 -*-

from mockmr.mockmr import PandasJob
import pandas as pd

class Averager(PandasJob):
    # Calculates the mean of the Age column of a dataframe

    def map_fn(self, _, subset_df):
        subset_mean = subset_df['Age'].mean()
        yield ("mean", subset_mean)

    def reduce_fn(self, key, values):
        list_values = list(values)
        yield (key, sum(list_values)/len(list_values))

dataframe = pd.read_csv('ages.csv')

job = Averager()

results = job.run(dataframe, n_chunks=4)

print(results)
