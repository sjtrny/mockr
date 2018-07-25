# -*- coding: utf-8 -*-

from itertools import chain
import collections
from collections import defaultdict
import pandas as pd

class MockJob(object):
    """
    MapReduce Job base class

    Notes
    -----
    This class is not intended for direct use. Use one of the Child classes or define your own.
    """

    def map_fn(self, key, chunk):
        """Abstract Map function. Inherit from one of the child classes and define your own.

        This function recieves a chunk of your input data.

        To pass data to a Reduce function you must yield at least one (key, value).

        Args:
            key (object): Key associated with the chunk, currently unused. This value will always be an empty string.
            chunk (object): Chunk, the object to be processed in this map function

        """
        raise NotImplementedError("Please implement this method")

    def reduce_fn(self, key, value):
        """Abstract Reduce Function. Inherit from one of the child classes and define your own.

        This function recieves data from the map function. To return final data your reduce function should yield at
        least one (key, output) pair.

        Args:
            key (object): Unique identifier for a set of values. This is set by one of your map functions.
            value (object): Output from a map function associated with key.

        """
        raise NotImplementedError("Please implement this method")

    def run(self, input_data, n_chunks = None):
        """Abstract Run function. Inherit from one of the child classes and define your own.

        This function runs the MapReduce job

        Args:
            input_data (object): Data to be processed, accepted types depends on sub-class definition
            n_chunks (int): Number of chunks to break data into, functionalty depends on sub-class
        """
        raise NotImplementedError("Please implement this method")

    def _run_job(self, chunks):
        """Runs the MapReduce job for this instance.

        Internal use only.

        Args:
            chunks (list): List of chunk objects
        """

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

class PythonJob(MockJob):
    """
    PythonJob base class

    Notes
    -----
    PythonJob class expects input to be a Collections.abc.Sequence type object e.g. Python List. Python Jobs provide two exection methods:

    - the sequence is divided into chunks and each chunk is sent to a separate map worker
    - each item in the list is individually sent to a dedicated map worker
    """


    def run(self, input_data, n_chunks = None):
        """Runs the MapReduce job for this instance.

        Notes
        -----
        - ``input_data`` must be able to divide evenly into chunk size pieces
        - you may wish to pre-shuffle your input_data

        Args:
            input_data (Collections.abc.Sequence): Sequence type holding data items e.g. Python ``list`` of ``str``
            n_chunks (int): The number of chunks to divide the input_data into. When ``n_chunks = None`` we assign a map
               function to each item of input_data. When ``n_chunks = int`` means that we create ``int`` chunks of data, each
               worker gets a chunk (sublist). ``input_data`` must be able to divide evenly into chunk size pieces
        """

        if not isinstance(input_data, collections.abc.Sequence) or isinstance(input_data, str):
            raise ValueError("input_data must be of type Sequence (i.e. indexable such as a list)")

        if n_chunks == None:
            return self._run_job(input_data)

        if not isinstance(n_chunks, int) or n_chunks <= 0:
            raise ValueError("n_chunks must be a positive int")

        # Divide input_data into chunks
        chunk_size = len(input_data) / n_chunks

        chunks = []
        for i in range(n_chunks):
            start_pos = int(i * chunk_size)
            end_pos = int(start_pos + chunk_size)
            current_chunk = input_data[start_pos:end_pos]
            chunks.append(current_chunk)

        return self._run_job(chunks)

class StreamingJob(MockJob):
    """
    StreamingJob base class

    Notes
    -----
    StreamingJob class expects the input to be a string. Newline (“n”) characters delimit “chunks” of data and each
     line/chunk is sent to a separate map worker.
    """

    def run(self, input_data, n_chunks = None):
        """Runs the MapReduce job for this instance.

        Args:
            input_data (str): newline delimited string, each string is assigned to a map worker
            n_chunks (int): ignored since the string is seperated by newlines and each string is treated as a chunk
        """
        if not isinstance(input_data, str):
            raise ValueError("input_data must be of type str")

        lines = input_data.split("\n")

        return self._run_job(lines)

class PandasJob(MockJob):
    """
    PandasJob base class

    Notes
    -----
    PandasJob class expects input to be a Pandas DataFrame.The rows of the data frame are equally divided into chunks
    and each chunk is sent to a separate map worker
    """

    def run(self, input_data, n_chunks = 4):
        """Runs the MapReduce job for this instance.

        Args:
            input_data (pandas.DataFrame): pandas ``DataFrame`` to be processed
            n_chunks (int): The number of chunks to divide the ``input_data`` into. ``input_data`` must be able to
               divide evenly into chunk size pieces
        """
        if not isinstance(input_data, pd.DataFrame):
            raise ValueError("input_str must be of type Pandas DataFrame")

        if not isinstance(n_chunks, int) or n_chunks <= 0:
            raise ValueError("n_chunks must be a positive int")

        # Divide input_data into chunks
        chunk_size = len(input_data) / n_chunks

        chunks = []
        for i in range(n_chunks):
            start_pos = int(i * chunk_size)
            end_pos = int(start_pos + chunk_size)
            current_chunk = input_data.iloc[start_pos:end_pos, :]
            chunks.append(current_chunk)

        return self._run_job(chunks)