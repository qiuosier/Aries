import unittest
import logging
from Aries.strings import AString
logger = logging.getLogger(__name__)


class TestAString(unittest.TestCase):
    def test_str_methods(self):
        """Tests calling methods of python str with AString"""
        # Tests cases for methods returning a string
        test_cases = [
            # (input_value, method_to_call, expected_output)
            ("test string", "title", "Test String"),
            ("test String", "lower", "test string"),
        ]
        for t in test_cases:
            value = t[0]
            attr = t[1]
            expected = t[2]
            x = AString(value)
            self.assertEqual(getattr(x, attr)(), expected, "Test Failed. Attribute: %s" % attr)

        # Test split() method, which returns a list
        arr = AString("test string").split(" ")
        self.assertTrue(isinstance(arr, list), "split() should return a list.")
        self.assertEqual(len(arr), 2, "split() should return a list of 2 elements.")
        self.assertEqual(arr[0], "test", "split() Error.")
        self.assertEqual(arr[1], "string", "split() Error.")

        # Test the partition() method, which returns a 3-tuple
        arr = AString("test partition string").partition("partition")
        self.assertTrue(isinstance(arr, tuple), "partition() should return a tuple.")
        self.assertEqual(len(arr), 3, "partition() should return a list of 3 elements.")
        self.assertEqual(arr[0], "test ", "partition() Error.")
        self.assertEqual(arr[1], "partition", "partition() Error.")
        self.assertEqual(arr[2], " string", "partition() Error.")

        # Test endswith() method
        self.assertTrue(AString("test string").endswith("ing"), "endswith() Error.")


class TestFileName(unittest.TestCase):
    pass
