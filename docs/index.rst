.. MockMR documentation master file, created by
   sphinx-quickstart on Tue Jul 24 16:59:43 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. https://wwoods.github.io/2016/06/09/easy-sphinx-documentation-without-the-boilerplate/

MockMR (Mock MapReduce)
====================

MockMR is a Python library for writing MapReduce jobs in an educational setting. It is intended to be used as a conceptual teaching tool.

MockMR provides an interface for defining and running MapReduce locally. Simply define your map and reduce functions, input your data and call the run function. Everything is run sequentially and locally.

.. note:: MockMR is only compatible with **Python 3**.


Installation
-----------------

Install via pip::

    $ pip install mockmr

Documentation
-----------------------------

Examples and specific information on classes and methods are below.

.. toctree::
   :maxdepth: 2

   examples
   api

Job Types
-----------------

Streaming Jobs
^^^^^^^^^^
StreamingJob class expects the input to be a string. Newline ("\n") characters delimit "chunks" of data and each line/chunk is sent to a separate map worker.

Pandas Jobs
^^^^^^^^^^
PandasJob class expects input to be a Pandas DataFrame. The rows of the data frame are equally divided into chunks and each chunk is sent to a separate map worker

Python Jobs
^^^^^^^^^^
PythonJob class expects input to be a Collections.abc.Sequence type object e.g. Python List. Python Jobs provide two exection methods:

- the sequence is divided into chunks and each chunk is sent to a separate map worker
- each item in the list is individually sent to a dedicated map worker

Example Usage
-----------------

The below example demonstrates counting the number of words in a corpus using MapReduce and the MockStreamingJob class.

More :ref:`examples here<examples>`.

.. literalinclude:: ../examples/streaming_str_ex.py
   :linenos:
   :lines: 3-
   :caption: examples/streaming_str_ex.py

Output::

    [('hello', 1), ('this', 1), ('is', 2), ('a', 1), ('sample', 1), ('string', 1), ('it', 1), ('very', 1), ('simple', 1), ('goodbye', 1)]

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`