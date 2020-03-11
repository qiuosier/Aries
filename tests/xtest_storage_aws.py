import logging
import os
import boto3
import sys
import traceback
import time
from botocore.exceptions import NoCredentialsError
try:
    from ..test import AriesTest
    from ..storage import StorageFolder, StorageFile
except:
    aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
    if aries_parent not in sys.path:
        sys.path.append(aries_parent)
    from Aries.test import AriesTest
    from Aries.storage import StorageFolder, StorageFile
    from Aries.storage.gs import GSObject
    from Aries.strings import Base64String
logger = logging.getLogger(__name__)


class TestGCStorage(AriesTest):
    """Contains test cases for Google Cloud Platform Storage.
    """
    AWS_CREDENTIALS = None

    TEST_BUCKET_NAME = "davelab-test"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        client = boto3.client('s3')
        try:
            client.list_buckets()
            cls.AWS_CREDENTIALS = True
        except NoCredentialsError as ex:
            print("AWS Credentials not found.")
            print("%s: %s" % (type(ex), str(ex)))
            traceback.print_exc()

    def setUp(self):
        # Skip test if AWS credentials are not found.
        if not self.AWS_CREDENTIALS:
            self.skipTest("AWS Credentials not found.")

    def test_parse_uri(self):
        """Tests parsing GCS URI
        """
        # File
        file_obj = StorageFile("s3://%s/test_file.txt" % self.TEST_BUCKET_NAME)
        self.assertEqual(file_obj.scheme, "s3")
        self.assertEqual(file_obj.path, "/test_file.txt")

        # Folder
        folder_obj = StorageFolder("s3://%s/test_folder" % self.TEST_BUCKET_NAME)
        self.assertEqual(folder_obj.uri, "s3://%s/test_folder/" % self.TEST_BUCKET_NAME)
        self.assertEqual(folder_obj.scheme, "s3")
        self.assertEqual(folder_obj.path, "/test_folder/")

        # Bucket root
        folder_obj = StorageFolder("s3://%s" % self.TEST_BUCKET_NAME)
        self.assertEqual(folder_obj.uri, "s3://%s/" % self.TEST_BUCKET_NAME)
        self.assertEqual(folder_obj.scheme, "s3")
        self.assertEqual(folder_obj.path, "/")


