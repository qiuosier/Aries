import os
import datetime
import logging
import threading
from io import FileIO
from abc import ABC
from .base import StorageObject, StoragePrefixBase, StorageIOSeekable
logger = logging.getLogger(__name__)


class BucketStorageObject(StorageObject):
    """Represents a cloud storage object associated with a bucket.
    This object may not correspond to an actual object in the bucket, e.g. a folder in Google or S3 bucket.
    """
    # Caches clients for each scheme
    cache_dict = dict()
    # Expiration time for each client
    cache_expire = dict()

    # Ensure that only one thread can initialize the client at one time
    # Multiple threads initializing the s3 client at the same time may cause a KeyError: 'credential_provider'
    # https://github.com/boto/boto3/issues/1592
    client_lock = threading.Lock()

    # The number of seconds before the client expires.
    CACHE_EXPIRE_SEC = 1200

    def __init__(self, uri):
        StorageObject.__init__(self, uri)
        self._client = None
        self._bucket = None
        self._blob = None

    @classmethod
    def get_cached(cls, obj_id, init_method):
        """Gets an unexpired object by obj_id from cache, creates one using init_method() if needed.
        """
        cached_obj = cls.cache_dict.get(obj_id)
        now = datetime.datetime.now()
        if cached_obj:
            client_expire = cls.cache_expire.get(obj_id)
            # Use the cached client if it is not expired.
            if client_expire and client_expire > now:
                return cached_obj
        obj = init_method()
        cls.cache_dict[obj_id] = obj
        cls.cache_expire[obj_id] = now + datetime.timedelta(seconds=cls.CACHE_EXPIRE_SEC)
        return obj

    def get_client(self):
        obj_id = self.scheme
        with self.client_lock:
            return self.get_cached(obj_id, self.init_client)

    def get_bucket(self):
        obj_id = "%s://%s" % (self.scheme, self.bucket_name)
        return self.get_cached(obj_id, self.init_bucket)

    @property
    def bucket_name(self):
        """The name of the Cloud Storage bucket as a string."""
        return self.hostname

    @property
    def client(self):
        if not self._client:
            self._client = self.get_client()
        return self._client

    @property
    def bucket(self):
        if not self._bucket:
            self._bucket = self.get_bucket()
        return self._bucket

    def is_file(self):
        """Determine if the object is a file.
        This will return False if the object does not exist or the object is a folder.
        """
        if self.path.endswith("/"):
            return False
        if not self.exists():
            return False
        return True

    def init_client(self):
        raise NotImplementedError()

    def init_bucket(self):
        raise NotImplementedError()

    def exists(self):
        raise NotImplementedError()


class CloudStoragePrefix(StoragePrefixBase, ABC):
    def blobs(self, delimiter=""):
        """All blobs with the same prefix as this object
        The type of blobs depends on the actual implementation of the blobs() method.
        The delimiter causes a list operation to roll up all the keys that share a common prefix into a single result.
        See Also: https://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html
        """
        raise NotImplementedError()


class CloudStorageIO(StorageIOSeekable):
    def __init__(self, uri):
        """
        """
        StorageIOSeekable.__init__(self, uri)

        # Path of the temp local file
        self.temp_path = None

        # Stores the temp local FileIO object
        self.__file_io = None

        # Cache the size information
        # TODO: use cached property
        self.__size = None

    @property
    def size(self):
        if not self.__size:
            if self.__file_io:
                return os.fstat(self.__file_io.fileno).st_size
            self.__size = self.get_size()
        return self.__size

    def seek(self, pos, whence=0):
        if self.__file_io:
            self._offset = self.__file_io.seek(pos, whence)
            return self._offset
        return self._seek(pos, whence)

    def tell(self):
        if self.__file_io:
            self._offset = self.__file_io.tell()
        return self._offset

    def local(self):
        """Creates a local copy of the file.
        """
        if not self.__file_io:
            file_obj = self.create_temp_file()
            # Download file if appending or updating
            if self.exists() and ('a' in self.mode or '+' in self.mode):
                self.download(file_obj)
            # Close the temp file and open it with FileIO
            file_obj.close()
            mode = "".join([c for c in self.mode if c in "rw+ax"])
            self.__file_io = FileIO(file_obj.name, mode)
            self.temp_path = file_obj.name
        return self

    def read(self, size=None):
        """Reads the file from the Google Cloud bucket to memory

        Returns: Bytes containing the contents of the file.
        """
        start = self.tell()
        if self.__file_io:
            self.__file_io.seek(start)
            b = self.__file_io.read(size)
        else:
            if not self.exists():
                raise FileNotFoundError("File %s does not exists." % self.uri)
            file_size = self.size
            # TODO: size unknown?
            if not file_size:
                return b""
            if start >= file_size:
                return b""
            end = file_size - 1
            if size:
                end = start + size - 1
            if end > file_size - 1:
                end = file_size - 1
            # logger.debug("Reading from %s to %s" % (start, end))
            b = self.read_bytes(start, end)
        self._offset += len(b)
        return b

    def write(self, b):
        """Writes data into the file.

        Args:
            b: Bytes data

        Returns: The number of bytes written into the file.

        """
        if self.closed:
            raise ValueError("write to closed file %s" % self.uri)
        # Create a temp local file
        self.local()
        # Write data from buffer to file
        self.__file_io.seek(self.tell())
        size = self.__file_io.write(b)
        self._offset += size
        return size

    def __rm_temp(self):
        if self.temp_path and os.path.exists(self.temp_path):
            os.unlink(self.temp_path)
        logger.debug("Deleted temp file %s of %s" % (self.temp_path, self.uri))
        self.temp_path = None
        return

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

    def close(self):
        """Flush and close the file.
        This method has no effect if the file is already closed.
        """

        if self._closed:
            return

        if self.__file_io:
            if not self.__file_io.closed:
                self.__file_io.close()
            self.__file_io = None

        if self.temp_path:
            logger.debug("Uploading file to %s" % self.uri)
            with open(self.temp_path, 'rb') as f:
                self.upload(f)
            # Remove __temp_file if it exists.
            self.__rm_temp()
            # Set _closed attribute
            self._closed = True

    @property
    def updated_time(self):
        raise NotImplementedError()

    def exists(self):
        raise NotImplementedError()

    def get_size(self):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

    def upload(self, from_file_obj):
        raise NotImplementedError()

    def download(self, to_file_obj):
        """Downloads the data to a file object
        Caution: This method does not call flush()
        """
        raise NotImplementedError()

    def read_bytes(self, start, end):
        """Reads bytes from position start to position end, inclusive
        """
        raise NotImplementedError()
