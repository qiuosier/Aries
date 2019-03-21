import os
import logging
from gcloud import storage
logger = logging.getLogger(__name__)
try:
    from ..storage import StorageObject, StorageFolder
except (SystemError, ValueError):
    import sys
    from os.path import dirname
    aries_parent = dirname(dirname(dirname(__file__)))
    if aries_parent not in sys.path:
        sys.path.append(aries_parent)
    from Aries.storage import StorageObject, StorageFolder


def parse_gcs_uri(gs_path):
    if isinstance(gs_path, str) and gs_path.startswith("gs://"):
        gs_path = gs_path.strip("/")
        bucket = gs_path.replace("gs://", "").split("/", 1)[0]
        prefix = gs_path.replace("gs://" + bucket, "").strip("/")
        return bucket, prefix
    return None, None


class GSObject(StorageObject):
    def __init__(self, gs_path):
        super(GSObject, self).__init__(gs_path)
        self._client = None
        self._bucket = None
        # The "prefix" for gcs does not include the beginning "/"
        if self.path.startswith("/"):
            self.prefix = self.path[1:]
        else:
            self.prefix = self.path
            
    def _get_bucket(self):
        self._client = storage.Client()
        self._bucket = self._client.get_bucket(self.bucket_name)

    @property
    def bucket(self):
        if not self._bucket:
            self._get_bucket()
        return self._bucket

    @property
    def gs_path(self):
        return self.uri

    @property
    def bucket_name(self):
        return self.hostname


class GSFolder(GSObject, StorageFolder):
    """Represents a Google Cloud Storage Folder

    Method Resolution Order: GSFolder, GSObject, StorageFolder, StorageObject
    """

    def __init__(self, gs_path):
        """

        Args:
            gs_path:

        """
        # super() will call the __init__() of StorageObject, StorageFolder and GSObject
        super(GSFolder, self).__init__(gs_path)

        # Make sure prefix ends with "/", otherwise it is not a "folder"
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"

    @property
    def folders(self):
        iterator = self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
        list(iterator)
        return list(iterator.prefixes)

    @property
    def files(self):
        return list(self.bucket.list_blobs(prefix=self.prefix, delimiter=None))


def upload_file_to_bucket(local_file_path, cloud_file_path, bucket_name):
    """Uploads a file to Google cloud bucket.

    Args:
        local_file_path (str): The local file, including full file path and filename (source).
        cloud_file_path (str): The Google cloud storage path (destination).
        bucket_name (str): The name of the Google cloud bucket

    Returns: True if the operation is successful

    """
    if not os.path.exists(local_file_path):
        logger.error("Failed to upload file to Google cloud. File not found: " + local_file_path)
        return False
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(cloud_file_path)
    with open(local_file_path, 'rb') as my_file:
        logger.debug("Uploading file to gs://%s/%s" % (bucket_name, cloud_file_path))
        blob.upload_from_file(my_file)
    return True


def upload_file_to_bucket_and_delete(local_file_path, cloud_file_path, bucket_name):
    """Uploads a file to Google cloud bucket, and delete the file if the upload is success.

    Args:
        local_file_path (str): The local file, including full file path and filename (source).
        cloud_file_path (str): The Google cloud storage path (destination).
        bucket_name (str): The name of the Google cloud bucket

    Returns: True if the operation is successful

    """
    upload_success = upload_file_to_bucket(local_file_path, cloud_file_path, bucket_name)
    if upload_success and os.path.exists(local_file_path):
        os.remove(local_file_path)
        logger.debug("Removed %s" % local_file_path)
    return upload_success


def get_file_in_bucket(bucket, file_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(file_path)
    if blob is None:
        blob = bucket.blob(file_path)
    return blob


def download_to_file(file_obj, cloud_file_path, bucket):
    """Downloads a file from the Google cloud bucket.

    Returns: True if the operation is successful

    """
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket)
    except Exception as ex:
        logger.error(str(ex))
        return False
    blob = bucket.blob(cloud_file_path)
    blob.download_to_file(file_obj)
    return True
