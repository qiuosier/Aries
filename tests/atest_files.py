"""Contains tests for the strings module.
"""
import datetime
import logging
import unittest

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.files import File, Markdown

logger = logging.getLogger(__name__)


class TestFile(unittest.TestCase):
    def test_load_json_file(self):
        """Tests loading a json file
        """
        # File exists
        json_file = os.path.join(os.path.dirname(__file__), "fixtures", "test.json")
        json_dict = File.load_json(json_file)
        self.assertIn("key", json_dict)
        self.assertIn("list", json_dict)
        self.assertIn("dict", json_dict)
        # File does not exist, no default
        json_dict = File.load_json(os.path.join(os.path.dirname(__file__), "no_exist.json"))
        self.assertEqual(json_dict, {})
        # File does not exist, with default
        json_dict = File.load_json(
            os.path.join(os.path.dirname(__file__), "no_exist.json"),
            {"default_key": "default_value"}
        )
        self.assertEqual(json_dict["default_key"], "default_value")


class TestMarkdown(unittest.TestCase):

    test_file = os.path.join(os.path.dirname(__file__), "fixtures", "test.md")

    def test_markdown_title(self):
        """Tests getting the title of a markdown file.
        """
        md = Markdown(self.test_file)
        self.assertEqual(md.title, "This is title")