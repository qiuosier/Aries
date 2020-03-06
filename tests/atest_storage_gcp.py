"""Contains tests for the Google Cloud (gcp) storage module.
"""
import logging
import unittest

import os
import sys
import time
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.storage import StorageFolder, StorageFile
from Aries.storage.gs import GSObject
from Aries.strings import Base64String
logger = logging.getLogger(__name__)


def setUpModule():
    """Configures the Google Application Credentials

    The test environment may store the content of the JSON key-file in "GOOGLE_CREDENTIALS".
    This function decodes and saves the JSON key-file into the local file system.

    """
    test_dir = os.path.dirname(__file__)
    json_file = os.path.join(test_dir, "..", "private", "gcp.json")
    # Use the b64 encoded content as credentials if "GOOGLE_CREDENTIALS" is set.
    credentials = os.environ.get("GOOGLE_CREDENTIALS")
    if credentials and credentials.startswith("ew"):
        if not os.path.exists(json_file):
            Base64String(credentials).decode_to_file(json_file)
    # Set "GOOGLE_APPLICATION_CREDENTIALS" if json file exists.
    if os.path.exists(json_file):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_file


class TestGCStorage(AriesTest):
    """Contains test cases for Google Cloud Platform Storage.
    """
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Removes test folder if it is already there
        StorageFolder.init("gs://aries_test/copy_test/").delete()
        time.sleep(1)
        StorageFile("gs://aries_test/abc.txt").delete()
        StorageFile("gs://aries_test/new_file.txt").delete()
        StorageFile("gs://aries_test/moved_file.txt").delete()
        StorageFile("gs://aries_test/local_upload.txt").delete()

    def setUp(self):
        # Skip test if "GOOGLE_APPLICATION_CREDENTIALS" is not found.
        # if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        #     self.skipTest("GCP Credentials not found.")
        time.sleep(1)

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
        # Object without "/"
        gs_obj = GSObject("gs://aries_test/test_folder")
        self.assertEqual(gs_obj.bucket_name, "aries_test")
        self.assertEqual(gs_obj.prefix, "test_folder")
        # Object with "/"
        gs_obj = GSObject("gs://aries_test/test_folder/")
        self.assertEqual(gs_obj.bucket_name, "aries_test")
        self.assertEqual(gs_obj.prefix, "test_folder/")
        # Folder without "/"
        gs_obj = StorageFolder("gs://aries_test/test_folder")
        self.assertEqual(gs_obj.uri, "gs://aries_test/test_folder/")
        self.assertEqual(gs_obj.bucket_name, "aries_test")
        self.assertEqual(gs_obj.raw.prefix, "test_folder/")

    def test_bucket_root(self):
        """Tests accessing google cloud storage bucket root.
        """
        # Access the bucket root
        self.assert_bucket_root("gs://aries_test")
        # self.assert_bucket_root("gs://aries_test/")

    def assert_bucket_root(self, gs_path):
        """Checks if the bucket root contains the expected folder and files.

        Args:
            gs_path (str): Google cloud storage path to the bucket root, e.g. "gs://bucket_name".
        """
        parent = StorageFolder(gs_path)
        # Test listing the folders
        folders = parent.folders
        self.assertEqual(len(folders), 1, str(folders))
        self.assertTrue(isinstance(folders[0], StorageFolder), "Type: %s" % type(folders[0]))
        self.assertEqual(folders[0].uri, "gs://aries_test/test_folder/")
        # Test listing the files
        files = parent.files
        self.assertEqual(len(files), 2)
        for file in files:
            self.assertTrue(isinstance(file, StorageFile), "Type: %s" % type(file))
            self.assertIn(file.uri, [
                "gs://aries_test/file_in_root.txt",
                "gs://aries_test/test_folder"
            ])

    def test_gs_folder(self):
        """Tests accessing a Google Cloud Storage folder.
        """
        # Access a folder in a bucket
        self.assert_gs_folder("gs://aries_test/test_folder")
        # self.assert_gs_folder("gs://aries_test/test_folder/")
        # StorageFolder methods
        gs_folder = StorageFolder("gs://aries_test/test_folder/")
        self.assertEqual(len(gs_folder.folder_paths), 1)
        self.assertEqual(len(gs_folder.file_paths), 1)

    def assert_gs_folder(self, gs_path):
        """Checks if a Google Cloud Storage folder contains the expected folders and files.

        Args:
            gs_path ([type]): [description]
        """
        # Test listing the folders
        parent = StorageFolder(gs_path)
        folders = parent.get_folder_attributes()
        self.assertTrue(parent.exists())
        # self.assertEqual(parent.size, 11)
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0], "gs://aries_test/test_folder/test_subfolder/")
        names = parent.folder_names
        self.assertEqual(len(names), 1)
        self.assertEqual(names[0], "test_subfolder")
        # Test listing the files
        files = parent.files
        self.assertEqual(len(files), 1)
        self.assertTrue(isinstance(files[0], StorageFile), "Type: %s" % type(files[0]))
        self.assertEqual(files[0].uri, "gs://aries_test/test_folder/file_in_folder.txt")
        names = parent.file_names
        self.assertEqual(len(names), 1)
        self.assertEqual(names[0], "file_in_folder.txt")

    def test_gs_read_seek(self):
        # GSFile instance
        with StorageFile.init("gs://aries_test/file_in_root.txt") as gs_file:
            self.assertEqual(gs_file.scheme, "gs")
            # self.assertEqual(str(type(gs_file).__name__), "GSFile")
            self.assertTrue(gs_file.seekable())
            self.assertTrue(gs_file.readable())
            self.assertEqual(gs_file.size, 34)

    def test_gs_file(self):
        """Tests accessing a Google Cloud Storage file.
        """
        # Test the blob property
        # File exists
        gs_file_exists = StorageFile("gs://aries_test/file_in_root.txt")
        self.assertFalse(gs_file_exists.is_gz())
        self.assertTrue(gs_file_exists.blob.exists())
        self.assertEqual(gs_file_exists.size, 34)
        # File does not exists
        gs_file_null = StorageFile("gs://aries_test/abc.txt")
        self.assertFalse(gs_file_null.blob.exists())

        # Test the read() method
        self.assertEqual(gs_file_exists.read(), b'This is a file in the bucket root.')
        with self.assertRaises(Exception):
            gs_file_null.read()

        # Test write into a new file
        with gs_file_null('w+b') as f:
            f.write(b"abc")
            f.seek(0)
            self.assertEqual(f.read(), b"abc")

        # File will be uploaded to bucket after closed.
        # Test reading from the bucket
        self.assertEqual(gs_file_null.read(), b"abc")
        gs_file_null.delete()

    def test_copy_and_delete_folder(self):
        source_path = "gs://aries_test/test_folder/"
        # Destination path ends with "/", the original folder name will be preserved.
        dest_path = "gs://aries_test/copy_test/"
        folder = StorageFolder(source_path)
        folder.copy(dest_path)
        copied = StorageFolder(dest_path)
        self.assertEqual(len(copied.files), 0)
        self.assertEqual(len(copied.folders), 1)
        self.assertEqual(len(copied.blobs()), 4, [b.name for b in copied.blobs()])
        self.assertEqual(copied.folder_names[0], "test_folder")
        # Delete the copied files
        copied.delete()
        self.assertEqual(len(copied.files), 0)
        self.assertEqual(len(copied.folders), 0)

        # Destination path does not end with "/", the original folder will be renamed.
        dest_path = "gs://aries_test/copy_test/new_name"
        folder = StorageFolder(source_path)
        folder.copy(dest_path)
        copied = StorageFolder(dest_path)
        self.assertEqual(len(copied.files), 1)
        self.assertEqual(len(copied.folders), 1)
        self.assertEqual(len(copied.blobs()), 4, [b.name for b in copied.blobs()])
        self.assertEqual(copied.folder_names[0], "test_subfolder")
        # Delete the copied files
        StorageFolder("gs://aries_test/copy_test/").delete()

    def test_copy_and_delete_prefix(self):
        # Copy a set of objects using the prefix
        source_path = "gs://aries_test/test_folder"
        dest_path = "gs://aries_test/copy_test/"
        objects = GSObject(source_path)
        objects.copy(dest_path)
        copied = StorageFolder(dest_path)
        self.assertEqual(len(copied.files), 1)
        self.assertEqual(len(copied.folders), 1)
        self.assertEqual(len(copied.blobs()), 5, [b.name for b in copied.blobs()])
        self.assertEqual(copied.folder_names[0], "test_folder")
        # Delete the copied files
        StorageFolder("gs://aries_test/copy_test/").delete()

    def test_copy_to_root_and_delete(self):
        # Source without "/"
        source_path = "gs://aries_test/test_folder/test_subfolder"
        # Destination is the bucket root, whether it ends with "/" does not matter.
        dest_path = "gs://aries_test"
        folder = StorageFolder(source_path)
        folder.copy(dest_path)
        copied = StorageFolder("gs://aries_test/test_subfolder/")
        self.assertEqual(len(copied.files), 1)
        self.assertEqual(len(copied.folders), 0)
        self.assertEqual(len(copied.blobs()), 2, [b.name for b in copied.blobs()])
        self.assertEqual(copied.file_names[0], "file_in_subfolder.txt")
        # Delete the copied files
        StorageFolder("gs://aries_test/test_subfolder/").delete()
        # With "/"
        source_path = "gs://aries_test/test_folder/test_subfolder"
        dest_path = "gs://aries_test/"
        folder = StorageFolder(source_path)
        folder.copy(dest_path)
        copied = StorageFolder("gs://aries_test/test_subfolder/")
        self.assertEqual(len(copied.files), 1)
        self.assertEqual(len(copied.folders), 0)
        self.assertEqual(len(copied.blobs()), 2, [b.name for b in copied.blobs()])
        self.assertEqual(copied.file_names[0], "file_in_subfolder.txt")
        # Delete the copied files
        StorageFolder("gs://aries_test/test_subfolder/").delete()

    def test_upload_from_file(self):
        gs_file = StorageFile("gs://aries_test/local_upload.txt")
        # Try to upload a file that does not exist.
        local_file_non_exist = os.path.join(os.path.dirname(__file__), "abc.txt")
        with self.assertRaises(FileNotFoundError):
            gs_file.load_from_file(local_file_non_exist)
        # Upload a file and check the content.
        local_file = os.path.join(os.path.dirname(__file__), "fixtures", "test_file.txt")
        gs_file.load_from_file(local_file)
        self.assertEqual(gs_file.read(), b'This is a local test file.\n')
        gs_file.delete()

    def test_create_and_move_blob(self):
        gs_file = StorageFile("gs://aries_test/new_file.txt")
        self.assertFalse(gs_file.blob.exists())
        gs_file.create()
        self.assertTrue(gs_file.blob.exists())
        dest = "gs://aries_test/moved_file.txt"
        gs_file.move(dest)
        self.assertFalse(gs_file.exists())
        dest_file = StorageFile(dest)
        self.assertTrue(dest_file.exists())
        dest_file.delete()

    def copy_from_http(self):
        storage_obj = StorageFile("https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf")
        gs_path = "gs://davelab_temp/qq6/test.pdf"
        storage_obj.copy("gs://davelab_temp/qq6/test.pdf")
        self.assertTrue(StorageFile(gs_path).exists())
        StorageFile(gs_path).delete()
