import os
import sys
import logging
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.outputs import CaptureOutput, StreamHandler


class TestLogging(AriesTest):
    def test_log_formatter(self):
        logger = logging.getLogger(__name__)
        with CaptureOutput() as out:
            logger.debug(["A", "B"])
            logger.debug(dict(a=1, b=2))
            logger.debug("ABC")
        list_log = out.logs[0].split("\n", 1)[1]
        dict_log = out.logs[1].split("\n", 1)[1]
        self.assertEqual(list_log, '[\n    "A",\n    "B"\n]')
        self.assertEqual(dict_log, '{\n    "a": 1,\n    "b": 2\n}')
        self.assertNotIn("\n", out.logs[2])
