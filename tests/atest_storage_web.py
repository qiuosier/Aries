"""Contains tests for the Google Cloud (gcp) storage module.
"""
import logging
import os
import sys

aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.storage import StorageFolder, StorageFile
from Aries.strings import Base64String
logger = logging.getLogger(__name__)


class TestWebStorage(AriesTest):
    """Contains test cases for HTTP and FTP.
    """

    test_folder_path = os.path.join(os.path.dirname(__file__), "fixtures", "test_folder")

    def test_http(self):
        """
        """
        # URL does not exist
        storage_obj = StorageFile("http://example.com/abc/")
        self.assertFalse(storage_obj.exists())

        # URL exists
        storage_obj = StorageFile("https://www.google.com")
        self.assertTrue(storage_obj.exists())

        # Download. Copy to local file.
        storage_obj = StorageFile("https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf")
        local_file_path = os.path.join(self.test_folder_path, "test.pdf")
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        storage_obj.copy(local_file_path)
        self.assertTrue(os.path.exists(local_file_path))
        self.assertGreater(StorageFile(local_file_path).size, 0)
        StorageFile(local_file_path).delete()
