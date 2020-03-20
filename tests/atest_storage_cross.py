"""Contains tests for the Google Cloud (gcp) storage module.
"""
import logging
import os
import sys
try:
    from ..test import AriesTest
    from ..storage import StorageFolder, StorageFile
except:
    aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
    if aries_parent not in sys.path:
        sys.path.append(aries_parent)
    from Aries.test import AriesTest
    from Aries.storage import StorageFolder, StorageFile
logger = logging.getLogger(__name__)


class TestStorageCrossPlatform(AriesTest):
    """Contains test cases for HTTP and FTP.
    """

    def test_md5(self):
        """
        """
        test_file = os.path.join(os.path.dirname(__file__), "fixtures", "links.md")
        # Local
        local_file = StorageFile(test_file)
        local_md5 = local_file.md5_hex
        self.assertIsNotNone(local_md5)

        # GCP
        gs_path = "gs://aries_test/links.md"
        local_file.copy(gs_path)
        gs_file = StorageFile(gs_path)
        self.assertEqual(local_md5, gs_file.md5_hex)
        gs_file.delete()

        # AWS
        s3_path = "s3://davelab-test/links.md"
        local_file.copy(s3_path)
        s3_file = StorageFile(s3_path)
        self.assertEqual(local_md5, s3_file.md5_hex)
        s3_file.delete()
