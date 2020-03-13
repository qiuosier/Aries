"""Contains classes for manipulating Google Cloud Storage Objects.

If you are not running in Google Compute Engine or App Engine,
authentication to Google Cloud Platform is required in order to use this module.

Authentication can be done by setting the "GOOGLE_APPLICATION_CREDENTIALS" environment variable
to JSON key-file.
In command line:
    $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
In python:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/keyfile.json"

See Also: https://googleapis.github.io/google-cloud-python/latest/core/auth.html

"""
import os
import logging
import warnings
import tempfile
from functools import wraps
from google.cloud import storage
from google.cloud.exceptions import ServerError
from ..strings import Base64String
from ..tasks import ShellCommand, FunctionTask
from .base import BucketStorageObject, StorageFolderBase, CloudStorageIO
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
            base_interval=20,
            retry_pattern='linear'
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
    def get_bucket(self):
        return self.client.get_bucket(self.bucket_name)

    @property
    def gs_path(self):
        return self.uri

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

    def list_files(self, delimiter=None):
        """Gets all files with the prefix as GSFile objects

        Returns (list):
        """
        from .io import StorageFile
        return [
            StorageFile("gs://%s/%s" % (self.bucket_name, b.name))
            for b in self.blobs(delimiter)
            if not b.name.endswith("/")
        ]

    @api_decorator
    def list_folders(self):
        from .io import StorageFolder
        iterator = self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
        list(iterator)
        return [
            StorageFolder("gs://%s/%s" % (self.bucket_name, p))
            for p in iterator.prefixes
        ]

    @api_decorator
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
        with self.client.batch():
            for blob in blobs:
                method(blob, *args, **kwargs)
                counter += 1
        return counter

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
            counter += self.batch_request(batch, method, *args,**kwargs)
        return counter

    @staticmethod
    def delete_blob(blob):
        blob.delete()

    @api_decorator
    def delete(self):
        """Deletes all objects with the same prefix."""
        counter = self.batch_operation(self.delete_blob)
        logger.debug("%d files deleted." % counter)
        return counter

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

    def copy_blob(self, blob, destination):
        new_name = str(blob.name).replace(self.prefix, destination.prefix, 1)
        if new_name != str(blob.name) or self.bucket_name != destination.bucket_name:
            self.bucket.copy_blob(blob, destination.bucket, new_name)

    @api_decorator
    def copy(self, to):
        """Copies folder/file in a Google Cloud storage directory to another one.

        Args:
            to (str): Destination Google Cloud Storage path.
            If the path ends with "/", e.g. "gs://bucket_name/folder_name/",
                the folder/file will be copied under the destination folder with the original name.
            If the path does not end with "/", e.g. "gs://bucket_name/new_name",
                the folder/file will be copied and renamed to the "new_name".

        Returns: None

        Example:
            GSFolder("gs://bucket_a/a/b/c/").copy("gs://bucket_b/x/y/z") will copy the following files
                gs://bucket_a/a/b/c/d/example.txt
                gs://bucket_a/a/b/c/example.txt
            to
                gs://bucket_b/x/y/z/d/example.txt
                gs://bucket_b/x/y/z/example.txt

        """
        # Check if the destination is a bucket root.
        # Prefix will be empty if destination is bucket root.
        # Always append "/" to bucket root.
        if not GSObject(to).prefix and not to.endswith("/"):
            to += "/"

        if self.prefix.endswith("/"):
            # The source is a folder if its prefix ends with "/"
            if to.endswith("/"):
                # If the destination ends with "/",
                # copy the folder under the destination
                to += self.name + "/"
            else:
                # If the destination does not end with "/",
                # rename the folder.
                to += "/"
        else:
            # Otherwise, it can be a file or an object or a set of filtered objects.
            if to.endswith("/"):
                # If the destination ends with "/",
                # copy all objects under the destination
                to += self.name
            else:
                # If the destination does not end with "/",
                # simply replace the prefix.
                pass

        destination = GSObject(to)

        source_files = self.blobs()
        if not source_files:
            logger.debug("No files in %s" % self.uri)
            return
        counter = self.batch_operation(self.copy_blob, destination)
        logger.debug("%d files copied." % counter)


class GSFolder(GSObject, StorageFolderBase):
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

    @api_decorator
    def __file_paths(self):
        return [
            "gs://%s/%s" % (self.bucket_name, b.name)
            for b in self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
            if not b.name.endswith("/")
        ]

    @property
    def size(self):
        """The size in bytes of all files in the folder.

        Returns (int): Size in bytes.

        """
        # size_bytes = 0
        # # Total size of files and folders
        # for c in [self.files, self.folders]:
        #     for f in c:
        #         s = f.size
        #         if not s:
        #             continue
        #         size_bytes += s
        # TODO: This command requires gsutil installed
        cmd = ShellCommand("gsutil du -s %s" % self.uri).run()
        arr = cmd.std_out.strip().split()
        if arr and str(arr[0]).isdigit():
            s = int(arr[0])
            logger.debug("%s %s Bytes." % (self.path, s))
            return s
        raise ValueError(
            "Failed to get the folder size:\n%s "
            "Make sure gsutil is install correctly." % cmd.std_out
        )

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

    def get_size(self):
        return self.blob.size

    def read_bytes(self, start, end):
        return api_call(self.blob.download_as_string, start=start, end=end)

    def download(self, to_file_obj):
        api_call(self.blob.download_to_file, to_file_obj)
        return to_file_obj

    def upload(self, from_file_obj):
        api_call(self.blob.upload_from_file, from_file_obj)
