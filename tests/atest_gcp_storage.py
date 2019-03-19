"""Contains tests for the gcp storage module.
"""
import logging
import unittest

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.gcp.storage import GSFolder
logger = logging.getLogger(__name__)


class TestGCStorage(unittest.TestCase):
    def test_gs_folder(self):
        """Tests GSFolder class."""
        # Access the bucket root
        parent = GSFolder("gs://aries_test")
        folders = parent.folders
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0], "test_folder/")
        parent = GSFolder("gs://aries_test/")
        folders = parent.folders
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0], "test_folder/")

        # Access a folder in a bucket
        parent = GSFolder("gs://aries_test/test_folder")
        folders = parent.folders
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0], "test_folder/test_subfolder/")
        parent = GSFolder("gs://aries_test/test_folder/")
        folders = parent.folders
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0], "test_folder/test_subfolder/")
