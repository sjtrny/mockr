.. _examples:

Examples
===================

Streaming
-----------------

String Input
^^^^^^^^^^^^^^^

Counting the number of times each word occurs in a "\n" (newline) delimited string.

.. literalinclude:: ../examples/streaming_str_ex.py
   :linenos:
   :lines: 3-
   :caption: examples/streaming_str_ex.py

Output::

    [('hello', 1), ('this', 1), ('is', 2), ('a', 1), ('sample', 1), ('string', 1), ('it', 1), ('very', 1), ('simple', 1), ('goodbye', 1)]

Text File Input
^^^^^^^^^^^^^^^

Counting the number of times each word occurs in a text file.

.. literalinclude:: ../examples/streaming_txtfile_ex.py
   :linenos:
   :lines: 3-
   :caption: examples/streaming_txtfile_ex.py

Output::

   [('the', 14697), ('project', 91), ('gutenberg', 92), ('ebook', 10), ('of', 6742), ('moby', 89), ('dick', 88)......]


Pandas
-----------------

Calculating the average of the "Age" column of the dataframe.

The Averager class runs with 4 chunks meaning that the dataframe is divided into four subsets. Each subset is sent to a different map worker.

The map worker calculates the average of its own subset. The reduce worker then collates all "mean" values into a global mean.

.. literalinclude:: ../examples/pandas_ex.py
   :linenos:
   :lines: 3-
   :caption: examples/pandas_ex.py

Output::

   [('mean', 30.090000000000003)]

Python Sequences (list etc)
-----------------

Counting the number of times each word occurs in a list of strings.

Processing Items Seperately (Dedicated Map Worker)
^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/python_ex.py
   :linenos:
   :lines: 3-
   :caption: examples/python_ex.py

Output::

   [('hello', 1), ('this', 1), ('is', 2), ('a', 1), ('sample', 1), ('string', 1), ('it', 1), ('very', 1), ('simple', 1), ('goodbye', 1)]

Processing Groups of Items (Shared Map Worker)
^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/python_chunks_ex.py
   :linenos:
   :lines: 3-
   :caption: examples/python_chunks_ex.py

Output::

   [('hello', 1), ('this', 1), ('is', 2), ('a', 1), ('sample', 1), ('string', 1), ('it', 1), ('very', 1), ('simple', 1), ('goodbye', 1)]