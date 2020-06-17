"""Contains classes for manipulating Google Cloud Storage Objects.

If you are not running in Google Compute Engine or App Engine,
authentication to Google Cloud Platform is required in order to use this module.

Authentication can be done by setting the "GOOGLE_APPLICATION_CREDENTIALS" environment variable
to JSON key-file.
In command line:
    $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
In python:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/keyfile.json"

See Also:
    https://googleapis.github.io/google-cloud-python/latest/core/auth.html
    https://googleapis.dev/python/storage/latest/
    https://github.com/googleapis/python-storage

"""
import os
import logging
import warnings
import tempfile
import base64
import binascii
from functools import wraps
from google.cloud import storage
from google.cloud.exceptions import ServerError
from ..strings import Base64String
from ..tasks import FunctionTask
from .base import StorageFolderBase
from .cloud import BucketStorageObject, CloudStoragePrefix, CloudStorageIO
logger = logging.getLogger(__name__)


def setup_credentials(env_name, to_json_file=None):
    """Configures the GOOGLE_APPLICATION_CREDENTIALS
    by saving the value of an environment variable to a JSON file.
    """
    # Use the b64 encoded content as credentials if "GOOGLE_CREDENTIALS" is set.
    credentials = os.environ.get(env_name)
    if credentials and credentials.startswith("ew"):
        if not to_json_file:
            temp_file = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
            temp_file.close()
            to_json_file = temp_file.name
        Base64String(credentials).decode_to_file(to_json_file)
    # Set "GOOGLE_APPLICATION_CREDENTIALS" if json file exists.
    if os.path.exists(to_json_file):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = to_json_file


def api_call(func=None, *args, **kwargs):
    """Makes API call and retry if there is an exception.
    This is designed to resolve the 500 Backend Error from Google.

    Args:
        func (callable): A function or method.

    Examples:
        api_call(self.bucket.get_blob, self.prefix)

    See Also: https://developers.google.com/drive/api/v3/handle-errors#resolve_a_500_error_backend_error
    """
    if not func:
        return None
        # logger.debug("Making API call: %s..." % func.__name__)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ResourceWarning)
        warnings.simplefilter("ignore", UserWarning)
        return FunctionTask(func, *args, **kwargs).run_and_retry(
            max_retry=3,
            exceptions=ServerError,
            base_interval=60,
            retry_pattern='linear',
            capture_output=False
        )


def api_decorator(method):
    """Decorator for making API call and retry if there is an exception.
    This is designed to resolve the 500 Backend Error from Google.
    When the decorated function is called, the function call will be retry if there is an exception.

    Examples:
        @api_decorator
        def exists(self):
            return self.blob.exists
    """
    # logger.debug("Decorating %s for API call..." % method.__name__)
    @wraps(method)
    def wrapper(*method_args, **method_kwargs):
        return api_call(method, *method_args, **method_kwargs)
    return wrapper


class GSObject(BucketStorageObject):
    """The base class for Google Storage Object.
    """
    MAX_BATCH_SIZE = 900

    @property
    def blob(self):
        """Gets or initialize a Google Cloud Storage Blob.

        Returns: A Google Cloud Storage Blob object.

        This does not check whether the object exists.
        Use blob.exists() to determine whether or not the blob exists.

        """
        if self._blob is None:
            # logger.debug("Getting blob: %s" % self.uri)
            file_blob = api_call(self.bucket.get_blob, self.prefix)
            if file_blob is None:
                # The following will not make an HTTP request.
                # It simply instantiates a blob object owned by this bucket.
                # See https://googleapis.github.io/google-cloud-python/latest/storage/buckets.html
                # #google.cloud.storage.bucket.Bucket.blob
                file_blob = self.bucket.blob(self.prefix)
            self._blob = file_blob
        return self._blob

    @api_decorator
    def init_client(self):
        return storage.Client()

    @api_decorator
    def init_bucket(self):
        return self.client.get_bucket(self.bucket_name)

    @property
    def gs_path(self):
        return self.uri

    @api_decorator
    def exists(self):
        """Determines if there is an actual blob corresponds to this object.
        """
        return self.blob.exists()

    @api_decorator
    def create(self):
        """Creates an empty blob, if the blob does not exist.

        Returns:
            Blob: The Google Cloud Storage blob.
        """
        blob = storage.Blob(self.prefix, self.bucket)
        if not blob.exists():
            blob.upload_from_string("")
        return blob

    def delete(self):
        self.delete_blob(self.blob)

    def copy(self, to):
        self.copy_blob(self.blob, to)

    def copy_blob(self, blob, to):
        """Copies a blob object in the bucket to a new location.

        Args:
            blob: A Google Cloud Storage Blob object in the bucket.
            to: URI of the new blob (gs://...).

        Returns: True if the blob is copied. Otherwise False.

        """
        destination = GSObject(to)
        new_name = str(blob.name).replace(self.prefix, destination.prefix, 1)
        if new_name != str(blob.name) or self.bucket_name != destination.bucket_name:
            self.bucket.copy_blob(blob, destination.bucket, new_name)
            return True
        return False

    @staticmethod
    def delete_blob(blob):
        blob.delete()


class GSPrefix(CloudStoragePrefix, GSObject):
    # @api_decorator
    def batch_request(self, blobs, method, *args, **kwargs):
        """Sends a batch request to run method of a batch of blobs.
        The "method" will be applied to each blob in blobs like method(blob, *args, **kwargs)

        Args:
            blobs: A list of blobs, to be processed in a SINGLE batch.
            method: The method for processing each blob.
            *args: Additional arguments for method.
            **kwargs: Keyword arguments for method.

        Returns:

        """
        if not blobs:
            return 0
        counter = 0
        try:
            with self.client.batch():
                for blob in blobs:
                    method(blob, *args, **kwargs)
                    counter += 1
        except ValueError as ex:
            # Suppress the no deferred request errors
            # This error occurs when there is no file/blob in the batch.
            if str(ex).strip() == "No deferred requests":
                return 0
            raise ex
        return counter

    # @api_decorator
    def batch_operation(self, method, *args, **kwargs):
        blobs = self.blobs()
        batch = []
        counter = 0
        for blob in blobs:
            batch.append(blob)
            if len(batch) > self.MAX_BATCH_SIZE:
                counter += self.batch_request(batch, method, *args, **kwargs)
                batch = []
        if batch:
            counter += self.batch_request(batch, method, *args, **kwargs)
        return counter

    @api_decorator
    def blobs(self, delimiter=None):
        """Gets the blobs in the bucket having the prefix.

        The returning list will contain object in the folder and all sub-folders

        Args:
            delimiter: Use this to emulate hierarchy.
            If delimiter is None, the returning list will contain objects in the folder and in all sub-directories.
            Set delimiter to "/" to eliminate files in sub-directories.

        Returns: A list of GCS blobs.

        See Also: https://googleapis.github.io/google-cloud-python/latest/storage/blobs.html

        """
        return list(self.bucket.list_blobs(prefix=self.prefix, delimiter=delimiter))

    @property
    def uri_list(self):
        """Gets all file URIs with the prefix
        """
        return [
            "gs://%s/%s" % (self.bucket_name, b.name)
            for b in self.blobs()
            if not b.name.endswith("/")
        ]

    @property
    def files(self):
        from .io import StorageFile
        storage_files = []
        for b in self.blobs("/"):
            if b.name.endswith("/"):
                continue
            storage_file = StorageFile("gs://%s/%s" % (self.bucket_name, b.name))
            storage_file.raw_io._blob = b
            storage_files.append(storage_file)
        return storage_files

    @property
    def folders(self):
        return self.list_folders()

    @api_decorator
    def list_folders(self):
        from .io import StorageFolder
        iterator = self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
        list(iterator)
        return [
            StorageFolder("gs://%s/%s" % (self.bucket_name, p))
            for p in iterator.prefixes
        ]

    def exists(self):
        return True if self.blob.exists() or self.objects else False

    @api_decorator
    def delete(self):
        """Deletes all objects with the same prefix."""
        counter = self.batch_operation(self.delete_blob)
        logger.debug("%d files deleted." % counter)
        return counter

    @api_decorator
    def copy(self, to, contents_only=False):
        """Copies folder/file in a Google Cloud storage directory to another one.

        Args:
            to (str): Destination Google Cloud Storage path.
            contents_only: Copies only the content of the folder. This applies only if the GSObject is a folder.
                Defaults to False, i.e. a folder (with the same name as this folder)
                will be created at the destination to contain the files.

        Returns: The number of files copied.

        Warnings:
            When the URI of GSObject ends with "/", i.e. it is a folder,
                use "contents_only" to indicate if a new folder should be created to contain all files copied.
            When the GSObject is a file or a set of filtered files with the same prefix:
                If to ends with slash ("/"), all files will be copied under the "to" folder.
                    folders partially in the prefix will be kept.
                If to does NOT end with slash, the prefix of all files will simply be replaced with the prefix in "to".
                See the following examples for more details.

        Example:
            Either
                GSFolder("gs://bucket_a/alpha/beta/").copy("gs://bucket_b/x/y")
            or
                GSFolder("gs://bucket_a/alpha/beta/").copy("gs://bucket_b/x/y/")
            will copy the following files:
                gs://bucket_a/alpha/beta/gamma/example.txt
                gs://bucket_a/alpha/beta/example.txt
            to
                gs://bucket_b/x/y/beta/gamma/example.txt
                gs://bucket_b/x/y/beta/example.txt

            Either
                GSFolder("gs://bucket_a/alpha/beta/").copy("gs://bucket_b/x/y", contents_only=True)
            or
                GSFolder("gs://bucket_a/alpha/beta/").copy("gs://bucket_b/x/y/", contents_only=True)
            will copy the following files:
                gs://bucket_a/alpha/beta/gamma/example.txt
                gs://bucket_a/alpha/beta/example.txt
            to
                gs://bucket_b/x/y/gamma/example.txt
                gs://bucket_b/x/y/example.txt

            Also
                GSFolder("gs://bucket_a/alpha/be").copy("gs://bucket_b/x/y/")
            will copy the following files:
                gs://bucket_a/alpha/beta/gamma/example.txt
                gs://bucket_a/alpha/beta/example.txt
            to
                gs://bucket_b/x/y/beta/gamma/example.txt
                gs://bucket_b/x/y/beta/example.txt

            However
                GSFolder("gs://bucket_a/alpha/be").copy("gs://bucket_b/x/y")
            will copy the following files:
                gs://bucket_a/alpha/beta/gamma/example.txt
                gs://bucket_a/alpha/beta/example.txt
            to
                gs://bucket_b/x/yta/gamma/example.txt
                gs://bucket_b/x/yta/example.txt

        """
        # Check if the destination is a bucket root.
        # Prefix will be empty if destination is bucket root.
        # Always append "/" to bucket root.
        if not GSObject(to).prefix and not to.endswith("/"):
            to += "/"

        if self.prefix.endswith("/"):
            # The source is a folder if its prefix ends with "/"
            if contents_only:
                to += "/"
            else:
                # Copy the contents into a folder with the same name.
                to = os.path.join(to, self.name) + "/"
        else:
            # Otherwise, it can be a file or an object or a set of filtered objects or a folder.
            if to.endswith("/"):
                # If the destination ends with "/",
                # copy all objects under the destination
                to += self.name
            else:
                # If the destination does not end with "/",
                # simply replace the prefix.
                pass
        # logger.debug("Copying files to %s" % to)
        source_files = self.blobs()
        if not source_files:
            logger.debug("No files in %s" % self.uri)
            return 0
        counter = self.batch_operation(self.copy_blob, to)
        logger.debug("%d files copied." % counter)
        return counter


class GSFolder(GSPrefix, StorageFolderBase):
    """Represents a Google Cloud Storage Folder

    Method Resolution Order: GSFolder, GSObject, StorageFolder, StorageObject
    """

    def __init__(self, uri):
        """Initializes a Google Cloud Storage Directory.

        Args:
            uri: The path of the object, e.g. "gs://bucket_name/path/to/dir/".

        """
        # super() will call the __init__() of StorageObject, StorageFolder and GSObject
        GSObject.__init__(self, uri)
        StorageFolderBase.__init__(self, uri)

        # Make sure prefix ends with "/", otherwise it is not a "folder"
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"

    def exists(self):
        return True if self.blob.exists() or self.file_paths or self.folder_paths else False

    @property
    def folder_paths(self):
        """Folders(Directories) in the directory.
        """
        return self.__folders_paths()

    @api_decorator
    def __folders_paths(self):
        iterator = self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
        list(iterator)
        return [
            "gs://%s/%s" % (self.bucket_name, p)
            for p in iterator.prefixes
        ]

    @property
    def file_paths(self):
        """Files in the directory
        """
        paths = self.__file_paths()
        return paths

    def __file_paths(self):
        return [
            "gs://%s/%s" % (self.bucket_name, b.name)
            for b in self.blobs("/")
            if not b.name.endswith("/")
        ]

    @api_decorator
    def filter_files(self, prefix):
        return [
            GSFile("gs://%s/%s" % (self.bucket_name, b.name))
            for b in self.bucket.list_blobs(prefix=os.path.join(self.prefix, prefix), delimiter='/')
            if not b.name.endswith("/")
        ]


class GSFile(GSObject, CloudStorageIO):
    def __init__(self, uri):
        """Represents a file on Google Cloud Storage as a file-like object implementing the IOBase interface.

        Args:
            uri:

        GSFile allows seek and read without opening the file.
        However, position/offset will be reset when open() is called.
        The context manager calls open() when enter.
        """
        GSObject.__init__(self, uri)
        CloudStorageIO.__init__(self, uri)

    @property
    def updated_time(self):
        return self.blob.updated

    @property
    def md5_hex(self):
        return binascii.hexlify(base64.urlsafe_b64decode(self.blob.md5_hash)).decode()

    def get_size(self):
        return self.blob.size

    def read_bytes(self, start, end):
        return api_call(self.blob.download_as_string, start=start, end=end)

    def download(self, to_file_obj):
        api_call(self.blob.download_to_file, to_file_obj)
        return to_file_obj

    def upload(self, from_file_obj):
        api_call(self.blob.upload_from_file, from_file_obj)
