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
import binascii
import logging
import warnings
from functools import wraps
from tempfile import NamedTemporaryFile
from google.cloud import storage
from ..tasks import ShellCommand, FunctionTask
from ..storage import StorageObject, StorageFolder, StorageFile
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
        return FunctionTask(func, *args, **kwargs).run_and_retry(max_retry=3, base_interval=20, retry_pattern='linear')


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
    def __init__(self, gs_path, *args, **kwargs):
        """Initializes a Google Cloud Storage Object.

        Args:
            gs_path: The path of the object, e.g. "gs://bucket_name/path/to/file.txt".

        """
        super(GSObject, self).__init__(gs_path)
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

    @api_decorator
    def exists(self):
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
        return [
            GSFile("gs://%s/%s" % (self.bucket_name, b.name))
            for b in self.blobs(delimiter)
            if not b.name.endswith("/")
        ]

    @api_decorator
    def list_folders(self):
        iterator = self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
        list(iterator)
        return [
            GSFolder("gs://%s/%s" % (self.bucket_name, p))
            for p in iterator.prefixes
        ]

    @api_decorator
    def delete(self):
        """Deletes all objects with the same prefix."""
        # This needs to be done before the batch.
        blobs = self.blobs()
        if not blobs:
            return
        with self.client.batch():
            for blob in blobs:
                blob.delete()

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
        with self.client.batch():
            for blob in source_files:
                new_name = str(blob.name).replace(self.prefix, destination.prefix, 1)
                if new_name != str(blob.name) or self.bucket_name != destination.bucket_name:
                    self.bucket.copy_blob(blob, destination.bucket, new_name)

        logger.debug("%d blobs copied" % len(source_files))

    @api_decorator
    def move(self, to):
        """Moves the objects to another location."""
        self.copy(to)
        self.delete()


class GSFolder(GSObject, StorageFolder):
    """Represents a Google Cloud Storage Folder

    Method Resolution Order: GSFolder, GSObject, StorageFolder, StorageObject
    """

    def __init__(self, gs_path):
        """Initializes a Google Cloud Storage Directory.

        Args:
            gs_path: The path of the object, e.g. "gs://bucket_name/path/to/dir/".

        """
        # super() will call the __init__() of StorageObject, StorageFolder and GSObject
        super(GSFolder, self).__init__(gs_path)
        if not self.uri.endswith("/"):
            self.uri += "/"

        # Make sure prefix ends with "/", otherwise it is not a "folder"
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"

    @property
    def folders(self):
        """Folders(Directories) in the directory.
        """
        return self.list_folders()

    @api_decorator
    def __files(self):
        return [
            GSFile("gs://%s/%s" % (self.bucket_name, b.name))
            for b in self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
            if not b.name.endswith("/")
        ]

    @property
    def files(self):
        """Files in the directory
        """
        return self.__files()

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

    def exists(self):
        return True if self.blob.exists() or self.files or self.folders else False

    @api_decorator
    def filter_files(self, prefix):
        return [
            GSFile("gs://%s/%s" % (self.bucket_name, b.name))
            for b in self.bucket.list_blobs(prefix=os.path.join(self.prefix, prefix), delimiter='/')
            if not b.name.endswith("/")
        ]


class GSFile(GSObject, StorageFile):
    def __init__(self, gs_path, mode='rb'):
        """Represents a file on Google Cloud Storage as a file-like object implementing the IOBase interface.

        Args:
            gs_path:

        GSFile allows seek and read without opening the file.
        However, position/offset will be reset when open() is called.
        The context manager calls open() when enter.
        """
        # super() will call the __init__() of StorageObject, StorageFolder and GSObject
        self.__offset = 0
        self.__closed = True
        self.__buffer = None
        self.__buffer_offset = None
        self.__temp_file = None
        self.__gz = None
        GSObject.__init__(self, gs_path, mode)
        StorageFile.__init__(self, gs_path, mode)

    @property
    def size(self):
        return self.blob.size

    @property
    def local_temp_path(self):
        if self.__temp_file:
            return self.__temp_file.name
        return None

    def upload_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found: %s" % file_path)

        with open(file_path, 'rb') as f:
            api_call(self.blob.upload_from_file, f)
        return True

    def is_gz(self):
        if self.__gz is None:
            if self.blob.size < 2:
                return False
            offset = self.tell()
            self.seek(0)
            b = binascii.hexlify(self.read(2))
            logger.debug("File begins with: %s" % b)
            self.seek(offset)
            self.__gz = b == b'1f8b'
        return self.__gz

    # The following implements the IOBase interface.
    # For seeking
    def seek(self, pos, whence=0):
        """Changes the read beginning position to byte offset pos.
        Args:
            pos (int): The number of bytes.
            whence (int):
                * 0 -- start of stream (the default); offset should be zero or positive
                * 1 -- current stream position; offset may be negative
                * 2 -- end of stream; offset is usually negative

        Returns:

        """
        # Run __append() to save and clear the buffer
        if self.__buffer:
            self.__append()

        if whence == 0:
            if pos < 0:
                raise ValueError("negative seek position %r" % (pos,))
            self.__offset = pos
        elif whence == 1:
            self.__offset = max(0, self.__offset + pos)
        elif whence == 2:
            self.__offset = max(0, self.size + pos)
        else:
            raise ValueError("whence must be 0, 1 or 2.")
        return self.__offset

    def __convert_bytes_and_strings(self, s):
        # Convert string to bytes if needed
        if 'b' in self.mode and isinstance(s, str):
            s = s.encode()

        # Convert bytes to string if needed
        if 'b' not in self.mode and isinstance(s, bytes):
            s = s.decode()

        return s

    # For reading
    def read(self, size=None):
        """Reads the file from the Google Cloud bucket to memory

        Returns: Bytes containing the contents of the file.
        """
        # Write buffer if there is data in buffer
        if self.__buffer:
            self.__append()
        # Read data from temp file if it exist.
        if self.__temp_file:
            self.__temp_file.seek(self.__offset)
            b = self.__temp_file.read(size)
            self.__offset = self.__temp_file.tell()
            return self.__convert_bytes_and_strings(b)
        elif self.blob.exists():
            # Read data from bucket
            blob_size = self.blob.size
            if self.__offset >= blob_size:
                return self.__convert_bytes_and_strings("")
            end = blob_size - 1
            if size:
                end = self.__offset + size - 1
            if end >= blob_size - 1:
                end = None
            logger.debug("Reading from %s to %s" % (self.__offset, end))
            b = api_call(self.blob.download_as_string, start=self.__offset, end=end)
            self.__offset = end + 1 if end else blob_size
            logger.debug("%s bytes" % len(b))
            return self.__convert_bytes_and_strings(b)
        return None

    def local(self):
        if not self.__temp_file:
            self.__temp_file = NamedTemporaryFile(delete=False)
            self.__temp_file = self.download()
        return self

    # For writing
    def __append(self):
        """Appends the data from buffer to temp file.
        """
        logger.debug("Writing buffer into file...offset=%s" % self.__buffer_offset)
        # Do nothing if there is no buffer.
        if not self.__buffer:
            return
        # Create a temp local file if it does not exist.
        self.local()
        # Write data from buffer to file
        self.__temp_file.seek(self.__buffer_offset)

        b = self.__convert_bytes_and_strings(self.__buffer)
        self.__temp_file.write(b)
        # Clear buffer
        self.__buffer = None
        self.__buffer_offset = None
        self.__offset += len(b)

    def download(self, to_file_obj=None):
        if not to_file_obj:
            if 'b' in self.mode:
                mode = 'w+b'
            else:
                mode = 'w+'
            to_file_obj = NamedTemporaryFile(mode, delete=False)
            logger.debug("Created temp file: %s" % to_file_obj.name)
        # Download the blob to temp file if it exists.
        if self.blob.exists():
            logger.debug("Downloading %s ..." % self.uri)
            api_call(self.blob.download_to_file, to_file_obj)
            to_file_obj.flush()
        return to_file_obj

    def write(self, b):
        """Writes data into the file.

        Args:
            b: Bytes or str data

        Returns: The number of bytes written into the file.

        """
        if self.closed:
            raise ValueError("write to closed file")
        if self.__buffer is None:
            self.__buffer_offset = self.__offset
            self.__buffer = b
        else:
            self.__buffer += b
        # Append the buffer to temp file if size is greater than 1MB
        buffer_size = len(self.__buffer)
        if buffer_size > 1024 * 1024:
            self.__append()
        self.__offset += len(b)
        return len(b)

    @api_decorator
    def flush(self):
        """Flush write buffers and upload the data to bucket.
        """
        self.__append()
        if self.__temp_file:
            self.__temp_file.seek(0)
            self.blob.upload_from_file(self.__temp_file)

    def close(self):
        """Flush and close the file.
        This method has no effect if the file is already closed.
        """
        if self.__closed:
            return
        try:
            self.flush()
        finally:
            # Remove __temp_file if it exists.
            if self.__temp_file:
                self.__temp_file.close()
                if os.path.exists(self.__temp_file.name):
                    os.unlink(self.__temp_file.name)
                logger.debug("Deleted temp file %s" % self.__temp_file)
                self.__temp_file = None
            self.__buffer = None
            # Set __closed attribute
            self.__closed = True

    def open(self, mode=None):
        """Opens the file for writing
        """
        super().open(mode)
        self.__closed = False
        self.__buffer = None
        self.__temp_file = None
        # Reset offset position when open
        if 'a' in self.mode:
            # Move to the end of the file if open in appending mode.
            self.seek(0, 2)
        elif 'w' in self.mode:
            # Delete the file if open in write mode.
            self.delete()
            self.__offset = 0
        else:
            self.__offset = 0
        return self

    def readable(self):
        if self.__buffer or self.__temp_file or self.exists():
            return True
        return False