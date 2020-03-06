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
from io import FileIO
from functools import wraps
from tempfile import NamedTemporaryFile
from google.cloud import storage
from google.cloud.exceptions import ServerError
from ..tasks import ShellCommand, FunctionTask
from .base import StorageIOSeekable, StorageObject, StorageFolderBase
logger = logging.getLogger(__name__)


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


class GSObject(StorageObject):
    """The base class for Google Storage Object.

    Attributes:
        prefix: The Google Cloud Storage prefix, which is the path without the beginning "/"
    """
    MAX_BATCH_SIZE = 900

    def __init__(self, gs_path):
        """Initializes a Google Cloud Storage Object.

        Args:
            gs_path: The path of the object, e.g. "gs://bucket_name/path/to/file.txt".

        """
        StorageObject.__init__(self, gs_path)
        self._client = None
        self._bucket = None
        # The "prefix" for gcs does not include the beginning "/"
        if self.path.startswith("/"):
            self.prefix = self.path[1:]
        else:
            self.prefix = self.path
        self.__blob = None

    def is_file(self):
        if self.path.endswith("/"):
            return False
        if not self.exists():
            return False
        return True

    @property
    def blob(self):
        """Gets or initialize a Google Cloud Storage Blob.

        Returns: A Google Cloud Storage Blob object.
            Use blob.exists() to determine whether or not the blob exists.

        """
        if self.__blob is None:
            file_blob = api_call(self.bucket.get_blob, self.prefix)
            if file_blob is None:
                # The following will not make an HTTP request.
                # It simply instantiates a blob object owned by this bucket.
                # See https://googleapis.github.io/google-cloud-python/latest/storage/buckets.html
                # #google.cloud.storage.bucket.Bucket.blob
                file_blob = self.bucket.blob(self.prefix)
            self.__blob = file_blob
        return self.__blob

    @property
    def bucket_name(self):
        """The name of the Google Cloud Storage bucket as a string."""
        return self.hostname

    @property
    def client(self):
        if not self._client:
            self._client = storage.Client()
        return self._client

    @api_decorator
    def _get_bucket(self):
        self._bucket = self.client.get_bucket(self.bucket_name)

    @property
    def bucket(self):
        if not self._bucket:
            self._get_bucket()
        return self._bucket

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


class GSFile(GSObject, StorageIOSeekable):
    def __init__(self, gs_path):
        """Represents a file on Google Cloud Storage as a file-like object implementing the IOBase interface.

        Args:
            gs_path:

        GSFile allows seek and read without opening the file.
        However, position/offset will be reset when open() is called.
        The context manager calls open() when enter.
        """
        GSObject.__init__(self, gs_path)
        StorageIOSeekable.__init__(self, gs_path)

        # Path of the temp local file
        self.temp_path = None

        # Stores the temp local FileIO object
        self.__temp_io = None

    @property
    def size(self):
        if self.__temp_io:
            return os.fstat(self.__temp_io.fileno).st_size
        return self.blob.size

    @property
    def updated_time(self):
        return self.blob.updated

    def upload_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found: %s" % file_path)

        with open(file_path, 'rb') as f:
            api_call(self.blob.upload_from_file, f)
        return True

    # For reading
    def read(self, size=None):
        """Reads the file from the Google Cloud bucket to memory

        Returns: Bytes containing the contents of the file.
        """
        start = self.tell()
        if self.__temp_io:
            self.__temp_io.seek(start)
            b = self.__temp_io.read(size)
        else:
            if not self.exists():
                raise FileNotFoundError("File %s does not exists." % self.uri)
            if not self.size:
                return b""
            # download_as_string() will raise an error if start is greater than size.
            if start > self.size:
                return b""
            end = None
            if size:
                end = start + size - 1
            logger.debug("Reading from %s to %s" % (start, end))
            b = api_call(self.blob.download_as_string, start=start, end=end)
        self._offset += len(b)
        return b

    def local(self):
        if not self.__temp_io:
            # Download file if appending or updating
            if 'a' in self.mode or '+' in self.mode:
                temp_file = self.download()
            else:
                temp_file = NamedTemporaryFile(delete=False)
            # Close the temp file and open it with FileIO
            temp_file.close()
            mode = "".join([c for c in self.mode if c in "rw+ax"])
            self.__temp_io = FileIO(temp_file.name, mode)
            self.temp_path = temp_file.name
        return self

    def download(self, to_file_obj=None):
        if not to_file_obj:
            to_file_obj = self.create_temp_file()
        # Download the blob to temp file if it exists.
        if self.blob.exists():
            logger.debug("Downloading %s ..." % self.uri)
            api_call(self.blob.download_to_file, to_file_obj)
            to_file_obj.flush()
        return to_file_obj

    def upload(self, from_file_obj):
        api_call(self.blob.upload_from_file, from_file_obj)

    def write(self, b):
        """Writes data into the file.

        Args:
            b: Bytes or str data

        Returns: The number of bytes written into the file.

        """
        if self.closed:
            raise ValueError("write to closed file")
        # Create a temp local file
        self.local()
        # Write data from buffer to file
        self.__temp_io.seek(self.tell())
        size = self.__temp_io.write(b)
        self._offset += size
        return size

    def __rm_temp(self):
        if self.temp_path and os.path.exists(self.temp_path):
            os.unlink(self.temp_path)
        logger.debug("Deleted temp file %s of %s" % (self.temp_path, self.uri))
        self.temp_path = None
        return

    def close(self):
        """Flush and close the file.
        This method has no effect if the file is already closed.
        """

        if self._closed:
            return

        if self.__temp_io:
            if not self.__temp_io.closed:
                self.__temp_io.close()
            self.__temp_io = None

        if self.temp_path:
            logger.debug("Uploading file to %s" % self.uri)
            api_call(self.blob.upload_from_filename, self.temp_path)
            # Remove __temp_file if it exists.
            self.__rm_temp()
            # Set _closed attribute
            self._closed = True

    def open(self, mode='r', *args, **kwargs):
        """Opens the file for writing
        """
        if not self._closed:
            self.close()
        super().open(mode)
        self._closed = False
        # Reset offset position when open
        self.seek(0)
        if 'a' in self.mode:
            # Move to the end of the file if open in appending mode.
            self.seek(0, 2)
        elif 'w' in self.mode:
            # Create empty local file
            self.local()
        return self

    def seek(self, pos, whence=0):
        if self.__temp_io:
            self._offset = self.__temp_io.seek(pos, whence)
            return self._offset
        return self._seek(pos, whence)

    def tell(self):
        if self.__temp_io:
            self._offset = self.__temp_io.tell()
        return self._offset
