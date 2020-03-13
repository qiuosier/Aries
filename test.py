"""Contains AriesTest class, a customized class to be used in place of unittest.TestCase.
"""
import logging
import time
import os
import sys
from collections import OrderedDict
from unittest import TestCase
from .outputs import StreamHandler, PackageLogFilter


class AriesTest(TestCase):
    """Customized TestCase:
        1. A stream handler is added to the root logger to output debug messages.
        2. Test outputs are buffered. Outputs for passing tests are discard.
            Only outputs for failed tests will be displayed.

        By default, the steam handler will be added to the root logger.
        This can be changed by override the "logger_names" class attribute 
            to a list of logger names, e.g. logger_names = ["tests"].

    Usage:
        This class can be used to replace unittest.TestCase.
        No other changes is needed.

    Remarks:
        Make sure super().setUpClass() is called when overriding this method in sub-classes.
    
    """
    # Stores the time for each test.
    # key: name of the test method.
    # value: time elapsed during the test.
    times = OrderedDict()

    # Override logger_names in subclasses to avoid enabling debug logging for root logger.
    logger_names = [""]

    decorated = False

    @classmethod
    def __decorate_test_case(cls, func):
        """A decorator for test case.
        1. Adds additional logging handler to test case so that logging will be streamed to stdout.
        2. Set logger level to DEBUG

        Args:
            func: test case function.

        Returns: A function.

        """
        def test_case_with_logging(*args, **kwargs):
            """Wraps the test cases and add a stream handler for logging
            """
            # At this point, unittest already replaced the sys.stdout
            # The sys.stdout passing into the StreamHandler is NOT the original system standard output
            # The default value (sys.stdout) for StreamHanlder() will send the logs to the system standard output
            stream_handler = StreamHandler(sys.stdout)
            package_filter = PackageLogFilter(packages=[os.path.basename(os.path.abspath(os.curdir))])
            stream_handler.addFilter(package_filter)
            loggers = dict()
            for name in cls.logger_names:
                logger = logging.getLogger(name)
                level = logger.getEffectiveLevel()
                loggers[logger] = level
                logger.setLevel(logging.DEBUG)
                logger.addHandler(stream_handler)
            try:
                # Run the function(test)
                results = func(*args, **kwargs)
            finally:
                for logger, level in loggers.items():
                    logger.removeHandler(stream_handler)
                    logger.setLevel(level)
            return results

        return test_case_with_logging

    @classmethod
    def setUpClass(cls):
        """Setup the test class.
        Each test case will be decorated so that the logger outputs are streamed to stdout.

        """
        super().setUpClass()
        if not cls.decorated:
            # Find all test cases
            attrs = dir(cls)
            for attr in attrs:
                if attr.startswith("test") and callable(getattr(cls, attr)):
                    test_case = getattr(cls, attr)
                    setattr(cls, attr, cls.__decorate_test_case(test_case))

            cls.decorated = True

    def run(self, result=None):
        """Runs the test case with standard output buffered.
        Output during a passing test is discarded.
        Output is echoed normally on test fail or error and is added to the failure messages.
        This is the same as running the Python unittest command line with "-b" option.
        
        See also:
        https://docs.python.org/3/library/unittest.html#command-line-options
        https://docs.python.org/3/library/unittest.html#unittest.TestResult.buffer

        Args:
            result:

        Returns:

        """
        if result is None:
            result = self.defaultTestResult()
        result.buffer = True
        start = time.time()
        returns = super().run(result)
        end = time.time()
        elapsed = end - start
        self.times[self._testMethodName] = elapsed
        return returns

