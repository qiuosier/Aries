"""Provides unified shortcuts/interfaces for access folders and files.
"""
import os
import logging
from abc import ABC
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse
from io import RawIOBase, UnsupportedOperation, SEEK_SET, DEFAULT_BUFFER_SIZE
logger = logging.getLogger(__name__)


class StorageObject:
    """Represents a storage object.
    This is the base class for storage folder and storage file.

    Attributes:
        path: the path of the object, which usually begins with slash.
        prefix: the prefix of the object, which does not contain the slash at the beginning of the path.

    """
    # Use a large buffer to improve performance of cloud storage access.
    BUFFER_SIZE = DEFAULT_BUFFER_SIZE * 128

    def __init__(self, uri):
        """Initializes a storage object.

        Args:
            uri (str): Uniform Resource Identifier for the object.
            The uri should include a scheme, except local files.

        See https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
        """
        self.uri = str(uri)
        parse_result = urlparse(self.uri)
        self.scheme = parse_result.scheme
        self.hostname = parse_result.hostname
        self.path = parse_result.path

        # Use file as scheme if one is not in the URI
        if not self.scheme:
            self.scheme = 'file'
            self.hostname = ""
            if not uri.startswith("/"):
                self.uri = os.path.abspath(self.uri)
            self.path = self.uri
            self.uri = "file://" + self.uri

        # The "prefix" for does not include the beginning "/"
        if self.path.startswith("/"):
            self.prefix = self.path[1:]
        else:
            self.prefix = self.path

    def __str__(self):
        """Returns the URI
        """
        return self.uri

    def __repr__(self):
        return self.uri

    @property
    def basename(self):
        """The basename of the file/folder, without path or "/".

        Returns:
            str: The basename of the file/folder
        """
        return os.path.basename(self.path.strip("/"))

    @property
    def name(self):
        """The basename of the file/folder, without path or "/".
        Same as basename.

        Returns:
            str: The basename of the file/folder
        """
        return self.basename

    @staticmethod
    def get_attributes(storage_objects, attribute):
        """Gets the attributes of a list of storage objects.

        Args:
            storage_objects (list): A list of Storage Objects, from which the values of an attribute will be extracted.
            attribute (str): A attribute of the storage object.

        Returns (list): A list of attribute values.

        """
        if not storage_objects:
            return []
        elif not attribute:
            return [str(f) for f in storage_objects]
        else:
            return [getattr(f, attribute) for f in storage_objects]

    @staticmethod
    def copy_stream(from_file_obj, to_file_obj):
        """Copies data from one file object to another
        """
        # TODO: there could be a problem if the file_obj has a different buffer size.
        chunk_size = StorageObject.BUFFER_SIZE
        file_size = 0
        while True:
            b = from_file_obj.read(chunk_size)
            if not b:
                break
            file_size += to_file_obj.write(b)
        to_file_obj.flush()
        return file_size

    def create_temp_file(self, delete=False, **kwargs):
        """Creates a NamedTemporaryFile on local computer with the same file extension.
        Everything after the first dot is considered as extension
        """
        # Determine the file extension
        if "suffix" not in kwargs:
            arr = self.basename.split(".", 1)
            if len(arr) > 1:
                suffix = ".%s" % arr[1]
                kwargs["suffix"] = suffix

        temp_obj = NamedTemporaryFile('w+b', delete=delete, **kwargs)
        logger.debug("Created temp file: %s" % temp_obj.name)
        return temp_obj


class StoragePrefixBase(StorageObject):
    @property
    def objects(self):
        """All storage files under this object, e.g. all files in the folder and sub-folders

        Returns: A list of storage file objects.

        """
        from .io import StorageFile
        return [StorageFile(uri) for uri in self.uri_list]

    @property
    def uri_list(self):
        """A list of URIs for each object with the same prefix
        """
        raise NotImplementedError()

    @property
    def size(self):
        """The size in bytes of all objects with the same prefix.

        Returns:

        This method requires the "object" to have a size attribute,
            sub-class should overwrite this if the size attribute is not available.

        AWS CLI: aws s3 ls --summarize --human-readable --recursive s3://bucket_name/path/to/folder | grep 'Total'
        GCP CLI: gsutil du -s gs://bucket_name/path/to/folder

        """
        total = 0
        for obj in self.objects:
            s = obj.size
            total += s if s else 0
        return total

    def exists(self):
        return True if self.objects else False

    def delete(self):
        for obj in self.objects:
            if not hasattr(obj, "delete") or not callable(obj.delete):
                raise UnsupportedOperation("Object %s does not support delete() method." % obj)
            obj.delete()


class StorageFolderBase(StorageObject):
    """Represents a folder/container of storage objects.
    This is the base class for folders or folder-like objects.
    It is used by
        Raw platform dependent classes, e.g. GSFolder, S3Folder, and
        IO interface class, i.e. StorageFolder
    When implementing raw, platform dependent sub-class:
        All the abstract methods should be implemented, otherwise a NotImplementedError will be raised:
            file_paths
            folder_paths
            exists()
            create()
            delete()
        Other methods (raising UnsupportedOperation error) can be implemented optionally to improve the performance:
            objects()
            copy()

    The io.StorageFolder class contains default implementations for methods like objects() and copy().
    In io.StorageFolder, when these methods are not implemented in the raw class,
        the UnsupportedOperation error will be caught and the default implementations will be used.
    """
    def __init__(self, uri):
        # Make sure uri ends with "/" for folders
        if uri and uri[-1] != '/':
            uri += '/'
        StorageObject.__init__(self, uri)

    @property
    def file_paths(self):
        """

        Returns: A list of URIs, each points to a file in the folder.

        """
        raise NotImplementedError()

    @property
    def folder_paths(self):
        """

        Returns: A list of URIs, each points to a folder in the folder.

        """
        raise NotImplementedError()

    def exists(self):
        """Checks if the folder exists.
        """
        raise NotImplementedError()

    def create(self):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

    @property
    def objects(self):
        raise UnsupportedOperation()

    def copy(self, to, contents_only=False):
        """Copies the folder to another location within the same scheme
        """
        raise UnsupportedOperation()


class StorageIOBase(StorageObject, RawIOBase):
    """Base class designed to provide:
        1. The underlying RawIO for a BufferedIO.
        2. High level operations like copy() and delete().

    StorageIOBase is an extension of the python RawIOBase
    See Also: https://docs.python.org/3/library/io.html#class-hierarchy

    The RawIO in StorageIOBase implementation is similar to the implementation of FileIO
    A sub-class of StorageIOBase can be used in place of FileIO
    Each sub-class should implement:
        read(), for reading bytes from the file.
        write(), for writing bytes into the file.
        close(), for closing the file.
        open(), should also be implemented if needed.

    In addition to interface provided by RawIOBase,
    StorageIOBase also defines some high level APIs.
    For high level operations, a sub-class should implement:
        size, the size of the file in bytes .
        exists(), determine if a file exists.
        delete(), to delete the file.
        load_from(), to load/create the file from a stream.

    Optionally, the following methods can be implemented
        to speed up the corresponding high-level operations.
        copy(), if a sub-class implements copy(), it should create the destination folder automatically when needed.
        local()
        upload()
        download()

    StorageIOBase and its sub-classes are intended to be the underlying raw IO of StorageFile.
    In general, they should not be used directly. The StorageFile class should be used instead.

    The file is NOT opened when initializing StorageIOBase with __init__().
    To open the file, call open() or use StorageIOBase.init().
    The close() method should be called after writing data into the file.
    Alternatively, the context manager can be used, e.g. "with StorageIOBase(uri) as f:"


    See Also:
        https://docs.python.org/3/library/io.html#io.FileIO
        https://docs.python.org/3/library/io.html#io.BufferedIOBase
        https://github.com/python/cpython/blob/1ed61617a4a6632905ad6a0b440cd2cafb8b6414/Lib/_pyio.py#L1461

    """
    def __init__(self, uri):
        StorageObject.__init__(self, uri)
        # Subclasses can use the following attributes
        self._closed = True
        # The following can be set by calling __set_mode(mode)
        # Raw IO always operates in binary mode
        self._mode = None
        self._created = False
        self._readable = False
        self._writable = False
        self._appending = False

    def __str__(self):
        """The URI of the file.
        """
        return self.uri

    def __call__(self, mode='rb'):
        self._set_mode(mode)
        return self

    @property
    def closed(self):
        return self._closed

    @property
    def mode(self):
        return self._mode

    def _set_mode(self, mode):
        """Sets attributes base on the mode.

        See Also: https://docs.python.org/3/library/functions.html#open

        """
        self._mode = mode
        # The following code is modified based on the __init__() of python FileIO class
        if not set(mode) <= set('xrwab+'):
            raise ValueError('Invalid mode: %s' % (mode,))
        if sum(c in 'rwax' for c in mode) != 1 or mode.count('+') > 1:
            raise ValueError('Must have exactly one of create/read/write/append '
                             'mode and at most one plus')

        if 'x' in mode:
            self._created = True
            self._writable = True
        elif 'r' in mode:
            self._readable = True
        elif 'w' in mode:
            self._writable = True
        elif 'a' in mode:
            self._writable = True
            self._appending = True
        if '+' in mode:
            self._readable = True
            self._writable = True

    def _is_same_mode(self, mode):
        if not self.mode:
            return False
        if mode:
            return sorted(self.mode) == sorted(mode)
        return True

    def open(self, mode='r', *args, **kwargs):
        if not self._is_same_mode(mode):
            self._set_mode(mode)
        self._closed = False
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return

    def _check_readable(self):
        """Checks if the file is readable, raise an UnsupportedOperation exception if not.
        """
        if not self.readable():
            raise UnsupportedOperation("File is not opened for read.")

    def _check_writable(self):
        """Checks if the file is writable, raise an UnsupportedOperation exception if not.
        """
        if not self.writable():
            raise UnsupportedOperation("File is not opened for write.")

    def writable(self):
        """Writable if file is writable and not closed.
        """
        return True if self._writable and not self._closed else False

    def readable(self):
        """Returns True if the file exists and readable, otherwise False.
        """
        if self._readable:
            return True
        return False

    def readall(self):
        """Reads all data from the file.

        Returns (bytes): All data in the file as bytes.

        """
        return self.read()

    def readinto(self, b):
        """Reads bytes into a pre-allocated bytes-like object b.

        Returns: An int representing the number of bytes read (0 for EOF), or
            None if the object is set not to block and has no data to read.

        This function is copied from FileIO.readinto()
        """
        # Copied from FileIO.readinto()
        m = memoryview(b).cast('B')
        data = self.read(len(m))
        n = len(data)
        try:
            m[:n] = data
        except Exception as e:
            logger.debug(n)
            logger.debug(len(m))
            raise e
        return n

    @property
    def size(self):
        return None

    @property
    def md5_hex(self):
        return None

    @property
    def updated_time(self):
        """Last updated/modified time of the file as a datetime object.
        """
        raise NotImplementedError()

    def close(self):
        self._closed = True
        raise NotImplementedError("close() is not implemented for %s" % self.__class__.__name__)

    def read(self, size=None):
        """Reads at most "size" bytes.

        Args:
            size: The maximum number of bytes to be returned

        Returns (bytes): At most "size" bytes from the file.
            Returns empty bytes object at EOF.

        """
        self._check_readable()
        raise NotImplementedError()

    def write(self, b):
        """Writes bytes b to file.

        Returns: The number of bytes written into the file.
            None if the write would block.
        """
        self._check_writable()
        raise NotImplementedError()

    def exists(self):
        """Checks if the file exists.
        """
        raise NotImplementedError("exists() is not implemented for %s" % self.__class__.__name__)

    def delete(self):
        raise NotImplementedError()

    def load_from(self, stream):
        """Creates/Loads the file from a stream
        """
        if self.closed:
            with self.open("wb") as f:
                file_size = self.copy_stream(stream, f)
        else:
            file_size = self.copy_stream(stream, self)
        return file_size

    def download(self, to_file_obj):
        raise UnsupportedOperation()

    def upload(self, from_file_obj):
        raise UnsupportedOperation()


class StorageIOSeekable(StorageIOBase, ABC):
    """Base class for seekable Storage
    Seekable storage sub-class should implement:
        seek()
        tell()

    This class has an _offset attribute to help keeping track of the read/write position of the file.
    A sub-class may not use the _offset attribute if the underlying IO keeps track of the position.
    However, if the _offset is used, the read() and write() in the sub-class are responsible to update the _offset.
    Otherwise the _offset will always be 0.

    _seek() provides a simple implementation of seek().
    
    """
    def __init__(self, uri):
        StorageIOBase.__init__(self, uri)
        self._offset = 0

    def seekable(self):
        return True

    def _seek(self, pos, whence=SEEK_SET):
        """Move to new file position.
        Argument offset is a byte count.  Optional argument whence defaults to
        SEEK_SET or 0 (offset from start of file, offset should be >= 0); other values
        are SEEK_CUR or 1 (move relative to current position, positive or negative),
        and SEEK_END or 2 (move relative to end of file, usually negative, although
        many platforms allow seeking beyond the end of a file).
        Note that not all file objects are seekable.
        """
        if not isinstance(pos, int):
            raise TypeError('pos must be an integer.')
        if whence == 0:
            if pos < 0:
                raise ValueError("negative seek position %r" % (pos,))
            self._offset = pos
        elif whence == 1:
            self._offset = max(0, self._offset + pos)
        elif whence == 2:
            self._offset = max(0, self.size + pos)
        else:
            raise ValueError("whence must be 0, 1 or 2.")
        return self._offset

    @property
    def size(self):
        """Returns the size in bytes of the file as an integer.
        """
        raise NotImplementedError()

    def seek(self, pos, whence=SEEK_SET):
        raise NotImplementedError()

    def tell(self):
        raise NotImplementedError()
