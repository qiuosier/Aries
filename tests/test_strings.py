import sys
from os.path import dirname
sys.path.append(dirname(dirname(dirname(__file__))))

import unittest
import logging
from Aries.strings import AString, FileName
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
            actual = getattr(x, attr)()
            self.assertEqual(type(actual), AString)
            self.assertEqual(str(actual), expected, "Test Failed. Attribute: %s" % attr)

        # Test split() method, which returns a list
        arr = AString("test string").split(" ")
        self.assertTrue(isinstance(arr, list), "split() should return a list.")
        self.assertEqual(len(arr), 2, "split() should return a list of 2 elements.")
        self.assertEqual(type(arr[0]), AString)
        self.assertEqual(type(arr[1]), AString)
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
    def test_filename_class(self):
        input_string = "abc.def"
        filename = FileName(input_string)
        self.assertEqual(type(filename), FileName)
        self.assertEqual(str(filename), input_string)

        basename = filename.basename
        self.assertEqual(str(basename), "abc")

        extension = filename.extension
        self.assertEqual(str(extension), "def")

        title = filename.title()
        self.assertEqual(type(title), FileName)
        self.assertEqual(str(title), "Abc.def")
        self.assertEqual(str(filename.upper()), "ABC.def")

        # Test appending random string
        n = 5
        filename_with_random = filename.title().append_random_letters(n)
        # len will only count the length of basename
        self.assertEqual(len(filename_with_random), len(input_string) + n - 3)
        self.assertIn(title.basename, str(filename_with_random))
        self.assertIn(title.extension, str(filename_with_random))
