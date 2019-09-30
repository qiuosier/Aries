# Outputs and Logging
The Aries outputs module provides classes and methods for logging and capturing outputs from python function or method. 

## Logging Configurations
The `Aries.output` module provide classes for configuring python logging in a few lines.

To configure logging, simply call `LoggingConfig.enable()` as follows:
```
import logging
from Aries.outputs import LoggingConfig

LoggingConfig().enable()
logger = logging.getLogger(__name__)
```
This will enable logging using the default logging message format.

The default logging format is:
```
'%(asctime)s | %(levelname)-8s | %(lineno)4d@%(module)-15s | %(message)s'
```
which produces logging messages like:
```
2019-09-29 22:57:03 | DEBUG    |   53@atest_outputs   | Test Config Debug
```
The line number and module name is useful for identifying the location of the logging code.

You can customize the logging format by passing a python logging [Formatter](https://docs.python.org/3/library/logging.html#formatter-objects) or a logging format string.
Logging format string generally contain placeholders of log record attributes. For more information about python logging format string. For more information, see [Python Log Record Attributes](https://docs.python.org/3/library/logging.html#logrecord-attributes).

Use the formatter argument to customize the formatter:
```
LoggingConfig(formatter='%(message)s').enable()
logger = logging.getLogger(__name__)
```

The `__name__` in variable is a special built-in variable containing the name of the module, e.g.(Aries.outputs). This is usually used as the name of the logger. However, you can replace it with your own string to name/organize your logging. This corresponds to the `%(name)s` attribute in a log record.

By default `LoggingConfig().enable()` changes the log level to DEBUG. You can modify this by passing a `level` argument.
```
LoggingConfig(level=logging.INFO).enable()
```

You may also want to output only the log messages from certain packages, for example, your own package. This can be archived by using the packages argument, which accepts a list of strings.
```
LoggingConfig(packages=["tests"]).enable()
```
The above code configures the logging to handle only logs from "tests" package.

To remove the logging configuration, save the object returned by `enable()` and call the `disable()` method.
```
config = LoggingConfig().enable()
# ...
# YOUR CODE HERE
# ...
config.disable()
```

Additionally, LoggingConfig also provides context manager and decorators for handling loggings for a particular part of your code.

Context Manager, logging configuration will be effective only inside "with":
```
with LoggingConfig():
    logger = logging.getLogger(__name__)
    # ...
    # YOUR CODE HERE
    # ...
# Logging will be restored here
```

Decorator, logging configuration will only be effective for the function:
```
@LoggingConfig.decorate
def your_function(*args, **kwargs):
    logger = logging.getLogger(__name__)
    # ...
    # YOUR CODE HERE
    # ...
```

## Capturing Outputs
The `CaptureOutput` class provides an easy way to capture the outputs and loggings of your code.
