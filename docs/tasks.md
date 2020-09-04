# Running Background Tasks with Multi-Threading
The `Aries.tasks` module provides mechanisms for running functions or shell commands in the background. They are designed to capture the outputs of functions and commands running asynchronously.

**Important**: This module is NOT designed to improving the performance with parallel processing. The Python Global Interpreter Lock (GIL) limits the performance of multi-threading. This module is designed for monitoring the executions of functions and commands, especially those depending on external resources (I/O or network).

Starting from Python 3.4, the `asyncio` module was added for writing concurrent code using the async/await syntax. `Aries.tasks` module does not use `asyncio`. The focus of `Aries.tasks` module is running and monitoring simple async tasks.

See also: 
* [Python’s GIL — A Hurdle to Multithreaded Program](https://medium.com/python-features/pythons-gil-a-hurdle-to-multithreaded-program-d04ad9c1a63)
* [Python Asynchronous I/O](https://docs.python.org/3/library/asyncio.html)

## Running a Function
Generally, a python function with positional argument and keyword arguments is defined as follows:
```
# This is the function to be executed
def func(*args, **kwargs):
    pass
    # More code here
```

The following example runs the function synchronously and captures the outputs and logs.
```
from Aries.tasks import FunctionTask

task = FunctionTask(func, *args, **kwargs)
# run() returns the return values of the function.
# The program is blocked until the function finishes running.
returns = task.run()
```

The `task` instance has the following attributes:
* std_out (str): Captured standard outputs.
* std_err (str): Captured standard errors.
* log_out (str): Captured log messages.
* exc_out (str): Captured exception outputs.
* returns: Return value of the task.

`FunctionTask` provides the `run_async()` method to run the function asynchronously:
```
task = FunctionTask(func, *args, **kwargs)
# run_async() returns a threading.Thread instance
# The program will continue while the function is running.
thread = task.run_async()

# Use join() to wait for the function
task.join()
```

For external I/O, network environments or services with high error rate, we may want to run the function and retry a few times if there is a particular exception. The following example will retry at most 5 times if there is a `ServerError` exception.
```
task = FunctionTask(func, *args, **kwargs)
returns = task.run_and_retry(max_retry=5, exception=ServerError)
```

For debugging purpose, the `run_profiler()` outputs (to the terminal) the detailed breakdown of execution times of function calls.
```
task = FunctionTask(func, *args, **kwargs)
task.run_profiler()
```

See also:
* [Python Debugging and Profiling](https://docs.python.org/3/library/debug.html)

## Running a Command
The `ShellCommand` class provides `run()` and `run_async()` methods, similar to `FunctionTask`. However, the `run()` method returns the task itself.
```
from Aries.tasks import ShellCommand

task = ShellCommand("ls -lG")
# run() returns the return values of the function.
# The program is blocked until the function finishes running.
task = task.run()
# The outputs of the ls command are stored in std_out.
task.std_out
```
