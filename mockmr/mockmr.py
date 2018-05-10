#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 14:02:58 2018

@author: Stephen Tierney
"""

from itertools import chain
from collections import defaultdict

class MockPythonJob(object):
    
    def map_fn(self, key, chunk):
        raise NotImplementedError("Please implement this method")
    
    def reduce_fn(self, key, value):
        raise NotImplementedError("Please implement this method")
    
    def run(self, chunks):
        # A list of processed lines
        m_generator_list = []
        
        # For each line call the map function
        # For each key call the reduce function
        
        for chunk in chunks:
            mapped_generator = self.map_fn("", chunk)
            m_generator_list.append(mapped_generator)
        
        m_generator_chain = chain.from_iterable(m_generator_list)
        
        # Key is the key, value is a list of the values
        shuffled_dict = defaultdict(list)
        
        for kv_tuple in m_generator_chain:
            shuffled_dict[kv_tuple[0]].append(kv_tuple[1])
        
        r_generator_list = []
        for k, v in shuffled_dict.items():
            gen_v = (n for n in v)
            reduced_generator = self.reduce_fn(k, gen_v)
            r_generator_list.append(reduced_generator)

        results = []
        for thing in r_generator_list:
            results.append(list(thing)[0])
            
            return results

class MockStreamingJob(object):

    def map_fn(self, key, line):
        raise NotImplementedError("Please implement this method")
    
    def reduce_fn(self, key, line):
        raise NotImplementedError("Please implement this method")
    
    def run(self, input_str):
        lines = input_str.split("\n")
    
        # A list of processed lines
        m_generator_list = []
        
        # For each line call the map function
        # For each key call the reduce function
        
        for line in lines:
            mapped_generator = self.map_fn("", line)
            m_generator_list.append(mapped_generator)
        
        m_generator_chain = chain.from_iterable(m_generator_list)
        
        # Key is the key, value is a list of the values
        shuffled_dict = defaultdict(list)
        
        for kv_tuple in m_generator_chain:
            shuffled_dict[kv_tuple[0]].append(kv_tuple[1]) 
        
        r_generator_list = []
        for k, v in shuffled_dict.items():
            gen_v = (n for n in v)
            reduced_generator = self.reduce_fn(k, gen_v)
            r_generator_list.append(reduced_generator)
         
        results = []
        for thing in r_generator_list:
            results.append(list(thing)[0])
    
        return results
