"""Contains tests for the strings module.
"""
import datetime
import logging

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.strings import AString, FileName

logger = logging.getLogger(__name__)


class TestAString(AriesTest):
    def test_str_methods(self):
        """Tests calling methods of python str with AString
        """
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

    def test_append_strings(self):
        """Tests appending strings
        """
        test_string = "test"
        a_string = AString(test_string)
        # Append date
        output = a_string.append_today()
        today = datetime.datetime.today()
        self.assertEqual(output, "%s_%s%s%s" % (
            test_string,
            str(today.year),
            str(today.month).zfill(2),
            str(today.day).zfill(2)
        ))

    def test_prepend_strings(self):
        """Tests prepending strings
        """
        test_string = "test"
        a_string = AString(test_string)
        # Prepend a list of strings
        prepend_list = ["a", "bc", "def"]
        output = a_string.prepend(prepend_list)
        self.assertEqual(str(output), "a_bc_def_test")
        # Prepend a single string
        self.assertEqual(a_string.prepend("abc"), "abc_test")

    def test_remove_non_alphanumeric(self):
        """Tests removing characters.
        """
        test_string = "!te\nst.123"
        self.assertEqual(
            AString(test_string).remove_non_alphanumeric(), 
            "test123"
        )

    def test_remove_escape_sequence(self):
        """Tests removing ANSI escape sequence.
        """
        test_string = "test\r\n\x1b[00m\x1b[01;31mHello.World\x1b[00m\r\n\x1b[01;31m"
        self.assertEqual(
            AString(test_string).remove_escape_sequence(), 
            "test\r\nHello.World\r\n"
        )

    def test_remove_non_ascii(self):
        """Tests removing non ASCII characters.
        """
        test_string = "!!te‘’st\n"
        self.assertEqual(
            AString(test_string).remove_non_ascii(), 
            "!!test\n"
        )

    def test_equality(self):
        """Tests equality operator.
        """
        a = AString("equal")
        b = AString("equal")
        self.assertEqual(a, b)
        self.assertEqual(AString(""), AString(None))


class TestFileName(AriesTest):

    @staticmethod
    def run_test(assert_method):
        test_base = "abc_def"
        test_extension = ".txt"
        assert_method(test_base, "")
        assert_method(test_base, test_extension)

    def test_properties(self):
        """Tests properties of the FileName class by initializing a FileName with extension.
        """
        self.run_test(self.assert_properties)

    def assert_properties(self, basename, extension):
        # Construct the test filename
        test_name = basename + extension
        # Make sure the test_name is a python string
        self.assertTrue(isinstance(test_name, str))
        filename = FileName(test_name)

        # Test converting FileName to string
        self.assertEqual(type(filename), FileName)
        self.assertEqual(str(filename), test_name)
        self.assertEqual(filename.to_string(), test_name)
        # Test basename
        self.assertEqual(str(filename.basename), basename)
        self.assertEqual(str(filename.name_without_extension), basename)
        # Test extension
        self.assertEqual(str(filename.extension), extension)

    def test_methods(self):
        """Tests the methods returning a FileName instance.
        """
        self.run_test(self.assert_methods)

    def assert_methods(self, basename, extension):
        # Construct the test filename
        test_name = basename + extension
        # Make sure the test_name is a python string
        self.assertTrue(isinstance(test_name, str))
        filename = FileName(test_name)

        # Test str method: title()
        title = filename.title()
        self.assertEqual(type(title), FileName)
        self.assertEqual(str(title), basename.title() + extension)
        # Test str method: upper()
        self.assertEqual(str(filename.upper()), basename.upper() + extension)

        # Test AString method: append_random_letters()
        n = 5
        filename_with_random = filename.title().append_random_letters(n)
        # Append adds an underscore between the random letters and the original filename.
        self.assertEqual(len(filename_with_random), len(test_name) + n + 1, "Filename: %s" % filename_with_random)
        self.assertIn(title.basename, str(filename_with_random))
        self.assertIn(title.extension, str(filename_with_random))

    def test_split(self):
        """Tests the split method, which returns a list.
        """
        self.run_test(self.assert_split)

    def assert_split(self, basename, extension):
        # Construct the test filename
        test_name = basename + extension
        # Make sure the test_name is a python string
        self.assertTrue(isinstance(test_name, str))
        filename = FileName(test_name)

        # Test str method: split()
        filename_splits = filename.split("_")
        self.assertEqual(len(filename_splits), 2)
        str_splits = basename.split("_")
        self.assertEqual(filename_splits[0], str_splits[0])
        self.assertEqual(filename_splits[1], str_splits[1])

    def test_operators(self):
        """Tests operators
        """
        self.run_test(self.assert_operators)

    def assert_operators(self, basename, extension):
        # Construct the test filename
        test_name = basename + extension
        # Make sure the test_name is a python string
        self.assertTrue(isinstance(test_name, str))
        filename = FileName(test_name)

        # Test "+" operator
        addition = "12345"
        new_filename = filename + addition
        self.assertEqual(str(new_filename), basename + extension + addition)

        # Test "*" operator
        multiplier = 2
        new_filename = filename * multiplier
        self.assertEqual(str(new_filename), (basename + extension) * 2)

        # Test slice
        new_filename = filename[2:4]
        self.assertEqual(str(new_filename), basename[2:4])

        # Test membership
        member = filename[2:4]
        self.assertTrue(member in filename)
