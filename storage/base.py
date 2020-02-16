"""Provides unified shortcuts/interfaces for access folders and files.
"""
import os
import logging
from urllib.parse import urlparse
from io import RawIOBase, UnsupportedOperation, SEEK_SET
logger = logging.getLogger(__name__)


class StorageObject:
    """Represents a storage object.
    This is the base class for storage folder and storage file.

    """

    def __init__(self, uri):
        """Initializes a storage object.

        Args:
            uri (str): Uniform Resource Identifier for the object.
            The uri should include a scheme, except local files.

        See https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
        """
        super(StorageObject, self).__init__()
        self.uri = str(uri)
        parse_result = urlparse(self.uri)
        self.hostname = parse_result.hostname
        self.path = parse_result.path
        self.scheme = parse_result.scheme
        # Use file as scheme if one is not in the URI
        if not self.scheme:
            self.scheme = 'file'

    def __str__(self):
        """Returns the URI
        """
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


class StorageIOBase(StorageObject, RawIOBase):
    """Base class designed to provide:
        1. The underlying RawIO for a BufferedIO.
        2. High level operations like copy() and delete().

    The RawIO implementation is similar to the implementation of FileIO
    Sub-class should implement:
        read()
        write()
        close()

    For high level operations, subclass should implement:
        size
        exists()
        delete()


    See Also:
        https://docs.python.org/3/library/io.html#io.FileIO
        https://docs.python.org/3/library/io.html#io.BufferedIOBase
        https://github.com/python/cpython/blob/1ed61617a4a6632905ad6a0b440cd2cafb8b6414/Lib/_pyio.py#L1461

    """

    def __init__(self, uri, mode='rb'):
        StorageObject.__init__(self, uri)
        self.mode = mode
        # Subclasses can use the following attributes
        self._closed = True
        # The following can be set by calling __set_mode(mode)
        self._created = False
        self._readable = False
        self._writable = False
        self._appending = False
        self.__set_mode(mode)

    def __str__(self):
        """The URI of the file.
        """
        return self.uri

    def __call__(self, mode='rb'):
        self.__set_mode(mode)
        return self

    def __set_mode(self, mode):
        """Sets attributes base on the mode.

        See Also: https://docs.python.org/3/library/functions.html#open

        """
        # The following code is modified based on the __init__() of python FileIO class
        if not isinstance(mode, str):
            raise TypeError('Invalid mode: %s' % (mode,))
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

    def exists(self):
        """Checks if the file exists.
        """
        raise NotImplementedError("exists() is not implemented for %s" % self.__class__.__name__)

    def readable(self):
        """Returns True if the file exists and readable, otherwise False.
        """
        if self.exists() and self._readable:
            return True
        return False

    def read(self, size=None):
        """Reads at most "size" bytes.

        Args:
            size: The maximum number of bytes to be returned

        Returns (bytes): At most "size" bytes from the file.
            Returns empty bytes object at EOF.

        """
        self._check_readable()
        raise NotImplementedError()

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
        m[:n] = data
        return n

    def writable(self):
        """Writable if file is writable and not closed.
        """
        return True if self._writable and not self._closed else False

    def write(self, b):
        """Writes bytes b to file.

        Returns: The number of bytes written into the file.
            None if the write would block.
        """
        self._check_writable()
        raise NotImplementedError()

    def open(self, mode=None):
        if mode:
            self.__set_mode(mode)
        self._closed = False
        return self

    def close(self):
        self._closed = True
        raise NotImplementedError("close() is not implemented for %s" % self.__class__.__name__)

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return

    @property
    def closed(self):
        return self._closed

    @property
    def size(self):
        """Returns the size in bytes of the file as an integer.
        """
        raise NotImplementedError("")


class StorageIOSeekable(StorageIOBase):
    """Base class for seekable Storage
    """
    def __init__(self, uri, mode):
        StorageIOBase.__init__(self, uri, mode)
        self._offset = 0

    def seek(self, pos, whence=SEEK_SET):
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

    def tell(self):
        return self._offset

    def seekable(self):
        return True
