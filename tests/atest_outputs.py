import os
import sys
import logging
from unittest import TestCase
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.web import WebAPI
from Aries.outputs import CaptureOutput, StreamHandler, LoggingConfig, PackageLogFilter


class TestLogging(AriesTest):
    def test_log_formatter(self):
        """Tests log formatter for list and dict.
        """
        logger = logging.getLogger(__name__)
        with CaptureOutput(suppress_exception=True) as out:
            logger.debug(["A", "B"])
            logger.debug(dict(a=1, b=2))
            logger.debug("ABC")
        list_log = out.logs[0].split("\n", 1)[1].strip("\n")
        dict_log = out.logs[1].split("\n", 1)[1].strip("\n")
        self.assertEqual(list_log, '[\n    "A",\n    "B"\n]')
        self.assertEqual(dict_log, '{\n    "a": 1,\n    "b": 2\n}')
        self.assertNotIn("\n", out.logs[2])

    def test_capturing_outputs(self):
        """Tests capturing outputs using the CaptureOutput class
        """
        logger = logging.getLogger(__name__)
        # Test setting the level to INFO and there should be no DEBUG log
        with CaptureOutput(log_level=logging.INFO) as out:
            logger.debug("Test Debug")
            logger.info(b"Test Info")
            print("Test Print")
        # There should be only one log since the level was set to INFO
        self.assertEqual(len(out.logs), 1, out.logs)
        self.assertTrue(out.logs[0].endswith("Test Info"), out.logs[0])
        self.assertEqual(out.std_out, "Test Print\n")


class TestConfigLogging(TestCase):
    """Tests here only checks whether the program executed without error.
    They do not check whether the logging outputs are correct.
    
    """
    def test_context_manager(self):
        """Tests config logging with context manager
        """
        logger = logging.getLogger(__name__)
        with LoggingConfig():
            logger.debug("Test Config Debug")
            logger.info("Test Config Info")
        logger.debug("Logging should not show up.")

    def test_enable_disable(self):
        logger = logging.getLogger(__name__)
        config = LoggingConfig(formatter='%(message)s').enable()
        logger.debug("Logging Enabled")
        config.disable()
        logger.debug("Logging should not show up.")
    
    def test_decorator(self):
        """Tests config logging with decorator.
        """
        @LoggingConfig.decorate
        def test_func(msg):
            logger = logging.getLogger(__name__)
            logger.debug("Test Decorator: %s" % msg)
        
        logger = logging.getLogger(__name__)
        logger.debug("Logging should not show up.")
        test_func("debug")

    def test_log_filter(self):
        """Test config logging with filter
        """
        package_filter = PackageLogFilter(packages="tests")
        with LoggingConfig(filters=[package_filter]):
            logger = logging.getLogger(__name__)
            logger.debug("tests package Debug")
            WebAPI("https://www.google.com").get("")
            logger.info("tests package Info")
        logger.debug("Logging should not show up.")
