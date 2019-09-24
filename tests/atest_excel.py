"""Contains tests for the excel module.
"""
import re
import logging
import string
import random
from shutil import copyfile

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.excel import ExcelFile

logger = logging.getLogger(__name__)


class TestExcelFile(AriesTest):
    __test_file = os.path.join(os.path.dirname(__file__), "fixtures", "excel_test_file.xlsx")
    test_file = __test_file.replace("excel_test_file.xlsx", "excel_test.xlsx")

    def setUp(self):
        super().setUp()
        # Make a copy of the test file.
        # The original test file should never be used directly
        copyfile(self.__test_file, self.test_file)

    def tearDown(self):
        super().tearDown()
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def assert_cell_value(self, file_path, cell, value):
        excel_file = ExcelFile(file_path)
        self.assertEqual(excel_file.worksheet[cell].value, value)


class TestReadExcelFile(TestExcelFile):

    def test_read_file_header(self):
        excel_file = ExcelFile(self.test_file)
        # Case sensitive
        self.assertTrue(excel_file.has_headers(["Header A", "Header AB", "header c"]))
        self.assertFalse(excel_file.has_headers(["header a", "header ab", "header c"]))
        # Case insensitive
        self.assertTrue(excel_file.has_headers(["header a", "header ab", "header c"], re.IGNORECASE))
        # Column index
        self.assertEqual(excel_file.column_index("header A"), 1)
        self.assertEqual(excel_file.column_index("Header AB"), 2)
        self.assertEqual(excel_file.column_index("Header A", case_sensitive=True), 1)
        self.assertEqual(excel_file.column_index("header A", case_sensitive=True), -1)
        # Use another row as index
        excel_file.set_headers(2)
        self.assertTrue(excel_file.has_headers(["row 1", "col 2"], re.IGNORECASE))

    def test_get_data_table(self):
        excel_file = ExcelFile(self.test_file)
        # Get data table with first row
        table = excel_file.get_data_table(skip_first_row=False)
        self.assertEqual(len(table), 3)
        self.assertEqual(len(table[0]), 3)
        # Get data table without first row
        table = excel_file.get_data_table()
        self.assertEqual(len(table), 2)


class TestWriteExcelFile(TestExcelFile):

    file_copy = TestExcelFile.test_file.replace("excel_test.xlsx", "excel_test_copy.xlsx")
    test_cell = 'C3'
    test_value = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

    def setUp(self):
        super().setUp()
        if os.path.exists(self.file_copy):
            os.remove(self.file_copy)

    def tearDown(self):
        super().tearDown()
        if os.path.exists(self.file_copy):
            os.remove(self.file_copy)

    def test_modify_file(self):
        copyfile(self.test_file, self.file_copy)
        excel_file = ExcelFile(self.file_copy)
        # Modify the file with a random value
        excel_file.worksheet[self.test_cell] = self.test_value
        excel_file.save()
        self.assert_cell_value(self.file_copy, self.test_cell, self.test_value)

    def test_save_as_another_file(self):
        excel_file = ExcelFile(self.test_file)
        excel_file.worksheet[self.test_cell] = self.test_value
        # Save a copy of the file
        excel_file.save(self.file_copy)
        self.assert_cell_value(self.file_copy, self.test_cell, self.test_value)

    def test_save_file_content(self):
        excel_file = ExcelFile(self.test_file)
        excel_file.worksheet[self.test_cell] = self.test_value
        # Save file content
        with open(self.file_copy, 'wb') as f:
            f.write(excel_file.content())
        self.assert_cell_value(self.file_copy, self.test_cell, self.test_value)

    def test_create_file(self):
        excel_file = ExcelFile()
        excel_file.worksheet[self.test_cell] = self.test_value
        # Save without a file path should raise an exception
        with self.assertRaises(Exception):
            excel_file.save()
        # Save with a file path
        excel_file.save(self.file_copy)
        self.assert_cell_value(self.file_copy, self.test_cell, self.test_value)

