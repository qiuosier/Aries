"""Provides unified shortcuts/interfaces for access folders and files.
"""
import os
import logging
from urllib.parse import urlparse
from io import RawIOBase, UnsupportedOperation, SEEK_SET, DEFAULT_BUFFER_SIZE
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

    @classmethod
    def init(cls, uri, mode='r', *args, **kwargs):
        raw_io = cls(uri)
        return raw_io.open(mode, *args, **kwargs)

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

    def open(self, mode='r', *args, **kwargs):
        if not self._is_same_mode(mode):
            self._set_mode(mode)
        self._closed = False
        return self

    def close(self):
        self._closed = True
        raise NotImplementedError("close() is not implemented for %s" % self.__class__.__name__)

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

    def readable(self):
        """Returns True if the file exists and readable, otherwise False.
        """
        if self.exists() and self._readable:
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
        m[:n] = data
        return n

    def read(self, size=None):
        """Reads at most "size" bytes.

        Args:
            size: The maximum number of bytes to be returned

        Returns (bytes): At most "size" bytes from the file.
            Returns empty bytes object at EOF.

        """
        self._check_readable()
        raise NotImplementedError()

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

    @property
    def mode(self):
        return self._mode

    def _is_same_mode(self, mode):
        if not self.mode:
            return False
        if mode:
            return sorted(self.mode) == sorted(mode)
        return True

    def exists(self):
        """Checks if the file exists.
        """
        raise NotImplementedError("exists() is not implemented for %s" % self.__class__.__name__)

    def delete(self):
        raise NotImplementedError()

    def load_from(self, stream):
        """Creates/Loads the file from a stream
        """
        chunk_size = DEFAULT_BUFFER_SIZE
        file_size = 0
        with self.open("w+b") as f:
            while True:
                b = stream.read(chunk_size)
                if not b:
                    break
                file_size += f.write(b)
        return file_size


class StorageIOSeekable(StorageIOBase):
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
        if self.exists():
            return True
        return False

    def seek(self, pos, whence=SEEK_SET):
        raise NotImplementedError

    def tell(self):
        raise NotImplementedError

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
        raise NotImplementedError("")
