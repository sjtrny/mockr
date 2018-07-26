[![Documentation Status](https://readthedocs.org/projects/mockmr/badge/?version=latest)](https://mockmr.readthedocs.io/en/latest/?badge=latest)

# MockMR

MockMR is a Python library for writing MapReduce jobs in an Educational setting. It is intended to be used as a
conceptual teaching tool.

MockMR provides an interface for defining and running MapReduce locally. Simply define your map and reduce functions,
input your data and call the run function. Everything is run sequentially and locally.

## Installation

    pip install mockmr

## Documentation

Full documentation available here [https://mockmr.readthedocs.io/](https://mockmr.readthedocs.io/)

### Streaming Jobs

StreamingJob class which expects the input to be a byte stream of characters. The chunks of data are separated by
newline ("\n") characters. Each line is sent to a separate map worker.

### Native Python Sequence Jobs

PythonJob class expects input to be a Collections.abc.Sequence type object e.g. Python List. Python Jobs provide two
exection methods:

- the sequence is divided into chunks and each chunk is sent to a separate map worker
- each item in the list is individually sent to a dedicated map worker

### Pandas Jobs

PandasJob class expects input to be a Pandas DataFrame. The rows of the data frame are equally divided into chunks and
each chunk is sent to a separate map worker


## Example Usage

    import re
    from mockmr.mockmr import run_stream_job

    WORD_RE = re.compile(r"[\w']+")

    def map_fn(chunk):
        # yield each word in the line
        for word in WORD_RE.findall(chunk):
            yield (word.lower(), 1)

    def reduce_fn(key, values):
        yield (key, sum(values))

    input_str = "Hello!\nThis is a sample string.\nIt is very simple.\nGoodbye!"

    results = run_stream_job(input_str, map_fn, reduce_fn)

    print(results)

Output:

    [('hello', 1), ('this', 1), ('is', 2), ('a', 1), ('sample', 1), ('string', 1), ('it', 1), ('very', 1), ('simple', 1), ('goodbye', 1)]