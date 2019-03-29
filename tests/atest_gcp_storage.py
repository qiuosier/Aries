"""Contains tests for the gcp storage module.
"""
import logging
import unittest

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.gcp.storage import GSObject, GSFolder, GSFile
logger = logging.getLogger(__name__)


class TestGCStorage(unittest.TestCase):

    def test_parse_uri(self):
        """Tests parsing GCS URI
        """
        # Bucket root without "/"
        gs_obj = GSObject("gs://aries_test")
        self.assertEqual(gs_obj.bucket_name, "aries_test")
        self.assertEqual(gs_obj.prefix, "")
        # Bucket root with "/"
        gs_obj = GSObject("gs://aries_test/")
        self.assertEqual(gs_obj.bucket_name, "aries_test")
        self.assertEqual(gs_obj.prefix, "")
        # Folder without "/"
        gs_obj = GSObject("gs://aries_test/test_folder")
        self.assertEqual(gs_obj.bucket_name, "aries_test")
        self.assertEqual(gs_obj.prefix, "test_folder")
        # Folder with "/"
        gs_obj = GSObject("gs://aries_test/test_folder/")
        self.assertEqual(gs_obj.bucket_name, "aries_test")
        self.assertEqual(gs_obj.prefix, "test_folder/")

    def test_bucket_root(self):
        """Tests accessing google cloud storage bucket root.
        """
        # Access the bucket root
        self.assert_bucket_root("gs://aries_test")
        self.assert_bucket_root("gs://aries_test/")

    def assert_bucket_root(self, gs_path):
        parent = GSFolder(gs_path)
        # Test listing the folders
        folders = parent.folders
        self.assertEqual(len(folders), 1)
        self.assertTrue(isinstance(folders[0], GSFolder), "Type: %s" % type(folders[0]))
        self.assertEqual(folders[0].uri, "gs://aries_test/test_folder/")
        # Test listing the files
        files = parent.files
        self.assertEqual(len(files), 2)
        for file in files:
            self.assertTrue(isinstance(file, GSFile), "Type: %s" % type(file))
            self.assertIn(file.uri, [
                "gs://aries_test/file_in_root.txt",
                "gs://aries_test/test_folder"
            ])

    def test_gs_folder(self):
        """Tests accessing google cloud storage folder.
        """
        # Access a folder in a bucket
        self.assert_gs_folder("gs://aries_test/test_folder")
        self.assert_gs_folder("gs://aries_test/test_folder/")

    def assert_gs_folder(self, gs_path):
        # Test listing the folders
        parent = GSFolder(gs_path)
        folders = parent.get_folders()
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0], "gs://aries_test/test_folder/test_subfolder/")
        # Test listing the files
        files = parent.files
        self.assertEqual(len(files), 1)
        self.assertTrue(isinstance(files[0], GSFile), "Type: %s" % type(files[0]))
        self.assertEqual(files[0].uri, "gs://aries_test/test_folder/file_in_folder.txt")

    def test_gs_file(self):
        gs_file = GSFile("gs://aries_test/file_in_root.txt")
        blob = gs_file.blob
        self.assertTrue(blob.exists())
        gs_file = GSFile("gs://aries_test/abc.txt")
        blob = gs_file.blob
        self.assertFalse(blob.exists())
