# -*- coding: utf-8 -*-

from itertools import chain
import collections
from collections import defaultdict
import pandas as pd
import inspect

def __abstract_map_fn(chunk):
    """Abstract Map function. Used internally to check user supplied function signatures.

    This function recieves a chunk of your input data.

    To pass data to a Reduce function you must yield at least one (key, value).

    Args:
        chunk (object): Chunk, the object to be processed in this map function

    """
    raise NotImplementedError("Please implement this method")

def __abstract_reduce_fn(key, values):
    """Abstract Reduce Function. Used internally to check user supplied function signatures.

    This function recieves data from the map function. To return final data your reduce function should yield at
    least one (key, output) pair.

    Args:
        key (object): Unique identifier for a set of values. This is set by one of your map functions.
        value (object): Output from a map function associated with key.

    """
    raise NotImplementedError("Please implement this method")

def __run_job(chunks, map_fn, reduce_fn):
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
        mapped_generator = map_fn(chunk)
        m_generator_list.append(mapped_generator)

    m_generator_chain = chain.from_iterable(m_generator_list)

    # Key is the key, value is a list of the values
    shuffled_dict = defaultdict(list)

    for kv_tuple in m_generator_chain:
        shuffled_dict[kv_tuple[0]].append(kv_tuple[1])

    r_generator_list = []
    for k, v in shuffled_dict.items():
        gen_v = (n for n in v)
        reduced_generator = reduce_fn(k, gen_v)
        r_generator_list.append(reduced_generator)

    results = []
    for thing in r_generator_list:
        results.append(list(thing)[0])

    return results

def __validate_functions(map_fn, reduce_fn):

    if inspect.signature(map_fn).parameters != inspect.signature(__abstract_map_fn).parameters:
        raise ValueError("Map function arguments do not match required function signature")

    if inspect.signature(reduce_fn).parameters != inspect.signature(__abstract_reduce_fn).parameters:
        raise ValueError("Reduce function arguments do not match required function signature")


def run_stream_job(input_data, map_fn, reduce_fn):
    """
    ``run_stream_job`` expects the input to be a string. Newline (“n”) characters delimit “chunks” of data and each
    line/chunk is sent to a separate map worker.

    Args:
        input_data (str): newline delimited string, each string is assigned to a map worker
        map_fn: a function with signature ``(chunk)`` that yields one or more ``(key, value)`` tuple. ``chunk`` will
            be a ``str``. The yielded ``key`` and ``value`` can be of any type, they will be passed to ``reduce_fn``.
        reduce_fn: a reduce function with signature ``(key, value)`` that yields a single ``(key, result)`` tuple
    """
    if not isinstance(input_data, str):
        raise ValueError("input_data must be of type str")

    __validate_functions(map_fn, reduce_fn)

    lines = input_data.split("\n")

    return __run_job(lines, map_fn, reduce_fn)

def run_sequence_job(input_data, map_fn, reduce_fn, n_chunks = None):
    """
    ``run_sequence_job`` expects ``input_data`` to be of type ``Collections.abc.Sequence`` e.g. Python List. Sequence Jobs provide two exection methods:
        - the sequence is divided into chunks and each chunk is sent to a separate map worker
        - each item in the list is individually sent to a dedicated map worker

    Notes:

    - you may wish to pre-shuffle your input_data

    Args:
        input_data (Collections.abc.Sequence): Sequence type holding data items e.g. Python ``list`` of ``str``.
        map_fn: a map function with signature ``(chunk)`` that yields one or more ``(key, value)`` tuple. When
            ``n_chunk = None`` then ``chunk`` will be a single item of ``input_data``. When  ``n_chunks = int`` then
            ``chunk`` will be a sub-sequence of ``input_data`` of length ``len(input_data)/n_chunks``.
        reduce_fn: a reduce function with signature ``(key, value)`` that yields a single ``(key, result)`` tuple.
        n_chunks (int): The number of chunks to divide the input_data into. ``input_data`` must be able to
           divide evenly into chunk size pieces. See ``map_fn`` for more details and
            defined behaviour.
    """

    if not isinstance(input_data, collections.abc.Sequence) or isinstance(input_data, str):
        raise ValueError("input_data must be of type Sequence (i.e. indexable such as a list)")

    __validate_functions(map_fn, reduce_fn)

    if n_chunks == None:
        return __run_job(input_data, map_fn, reduce_fn)

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

    return __run_job(chunks, map_fn, reduce_fn)


def run_pandas_job(input_data, map_fn, reduce_fn, n_chunks = 4):
    """
    ``run_pandas_job`` expects input to be a Pandas DataFrame.The rows of the data frame are equally divided into chunks
    and each chunk is sent to a separate map worker.

    Notes:

    - you may wish to pre-shuffle your input_data

    Args:
        input_data (pandas.DataFrame): pandas ``DataFrame`` to be processed.
        map_fn: a function with signature ``(chunk)`` that maps ``input_data`` to one or more ``(key, value)`` tuples,
            which are emitted via yield. ``chunk`` will be a ``pandas.DataFrame`` that is a row-wise section of
            ``input_data``. The yielded ``key`` and ``value`` can be of any type, they will be passed to ``reduce_fn``.
            reduce_fn: a function with signature ``(key, value)`` that reduces one or more ``key, value`` pairs into a
            single ``(key, result)`` tuple which is emitted via yield.
        n_chunks (int): The number of chunks to divide the ``input_data`` into. ``input_data`` must be able to
           divide evenly into chunk size pieces.
    """
    if not isinstance(input_data, pd.DataFrame):
        raise ValueError("input_str must be of type Pandas DataFrame")

    __validate_functions(map_fn, reduce_fn)

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

    return __run_job(chunks, map_fn, reduce_fn)

