# MockMR

MockMR is a Python library for writing MapReduce jobs in an Educational setting. It is intended to be used as a conceptual teaching tool.

MockMR provides an interface for defining and running MapReduce locally. Simply define your map and reduce functions, input your data and call the run function. Everything is run sequentially and locally.

### Streaming Jobs

MockMR defines the MockStreamingJob class which expects the input to be a byte stream of characters.

The chunks of data are separated by newline ("\n") characters. Each line is sent to a seperate map worker.

### Native Python Jobs

MockMR defines the MockPythonJob class which accepts a Python list as input. The elements of the list can be any type.