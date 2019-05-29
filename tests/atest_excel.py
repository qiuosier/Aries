"""Contains tests for the strings module.
"""
import re
import logging
import unittest

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.excel import ExcelFile

logger = logging.getLogger(__name__)


class TestExcelFile(unittest.TestCase):
    def test_file_header(self):
        test_file = os.path.join(aries_parent, "Aries", "tests", "fixtures", "excel_test_file.xlsx")
        excel_file = ExcelFile(test_file)
        # Case sensitive
        self.assertTrue(excel_file.has_headers(["Header A", "Header AB", "header c"]))
        # Case insensitive
        self.assertTrue(excel_file.has_headers(["header a", "header ab", "header c"], re.IGNORECASE))
        # Column index
        self.assertEqual(excel_file.column_index("header A"), 1)
        self.assertEqual(excel_file.column_index("Header AB"), 2)
        self.assertEqual(excel_file.column_index("header A", case_sensitive=True), -1)
