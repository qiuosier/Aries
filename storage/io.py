"""Provides unified high level IO interface for accessing folders and files.
"""
import os
import json
import logging
from io import SEEK_SET, DEFAULT_BUFFER_SIZE
from io import BufferedIOBase, BufferedRandom, BufferedReader, BufferedWriter, TextIOWrapper
from .base import StorageObject, StorageIOBase
logger = logging.getLogger(__name__)


class StorageFolder(StorageObject):
    """Represents a storage folder.
    The path of a StorageFolder will always end with "/"

    """

    def __init__(self, uri):
        super(StorageFolder, self).__init__(uri)
        # Make sure path ends with "/"
        if self.path and self.path[-1] != '/':
            self.path += '/'

    @staticmethod
    def init(uri):
        """Opens a StorageFile as one of the subclass base on the URI.
        """
        from . import LocalFolder
        from .gs import GSFolder
        uri = str(uri)
        if uri.startswith("/") or uri.startswith("file://"):
            logger.debug("Using local folder: %s" % uri)
            return LocalFolder(uri)
        elif uri.startswith("gs://"):
            logger.debug("Using GS folder: %s" % uri)
            return GSFolder(uri)
        return StorageFolder(uri)

    @staticmethod
    def _get_attribute(storage_objects, attribute):
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

    @property
    def files(self):
        """

        Returns: A list of StorageFiles in the folder.

        """
        raise NotImplementedError

    @property
    def folders(self):
        """

        Returns: A list of StorageFolders in the folder.

        """
        raise NotImplementedError

    def get_files(self, attribute=None):
        """Gets a list of files (represented by uri or other attribute) in the folder.

        Args:
            attribute: The attribute of the StorageFile to be returned in the list representing the files.

        Returns: A list of objects, each represents a file in this folder.
            If attribute is None, each object in the returning list will be the URI of the StorageFile
            If attribute is specified, each object in the returning list will be the attribute value of a StorageFile.

        """
        return self._get_attribute(self.files, attribute)

    def get_folders(self, attribute=None):
        """Gets a list of folders (represented by uri or other attribute) in the folder

        Args:
            attribute: The attribute of the StorageFolder to be returned in the list representing the folders.

        Returns: A list of objects, each represents a folder in this folder.
            If attribute is None, each object in the returning list will be the URI of the StorageFolder
            If attribute is specified, each object in the returning list will be the attribute value of a StorageFolder.

        """
        return self._get_attribute(self.folders, attribute)

    @property
    def file_paths(self):
        return self.get_files()

    @property
    def folder_paths(self):
        return self.get_folders()

    @property
    def file_names(self):
        return self.get_files("name")

    @property
    def folder_names(self):
        return self.get_folders("name")

    def exists(self):
        """Checks if the folder exists.
        """
        raise NotImplementedError()

    def create(self):
        """Creates a new folder.
        There should be no error if the folder already exists.
        """
        return self

    def filter_files(self, prefix):
        raise NotImplementedError


class StorageFile(StorageObject, BufferedIOBase):
    """Represents a storage file.

    StorageFile() is designed be used in place of the python open()
    to obtain a file-like object for a file.
    See Also: https://docs.python.org/3/glossary.html#term-file-object

    Base on the scheme of the URI, StorageFile uses an sub-class of StorageIOBase as the underlying raw IO
    For binary mode, the raw IO is wrapped by BufferedIO
    For text mode, the bufferedIO is wrapped by TextIO

    Although StorageFile has raw_io as attribute, most operations should be done with the buffered_io

    """

    def __init__(self, uri, mode='r', buffering=-1, encoding=None, errors=None,
                 newline=None, closefd=True, opener=None):
        """Opens a file

        The arguments are the same as the one in python build-in open().
        Except that uri is used instead of file.

        Args:
            uri (str): The URI of the file, like file:///path/to/file or /path/to/file.
                If the URI does not contain a scheme, "file://" will be used.
            mode (str): The mode in which the file is opened,
                must be the combination of r, w, x, a, b, t and +
            buffering (int): Buffering policy.
            encoding (str): Name of the encoding.
            errors (str): The way to handle encoding error, must be:
                'strict': (Default) Raise a ValueError exception
                'ignore': Ignore errors.
            newline (str): New line character, which can be '', '\n', '\r', and '\r\n'.
            closefd (bool): This option currently has no effect.
                The underlying file descriptor will always be closed.
            opener: Custom opener.
        """
        StorageObject.__init__(self, uri)
        # Stores the arguments as protected attributes
        self._mode = str(mode)
        # The raw_io will be initialized in __init_io()
        self.raw_io = None
        # The buffered_io is the main IO stream used in the methods and properties.
        self.buffered_io = self.__init_io(buffering, encoding, errors, newline, closefd, opener)

    def __call__(self, mode='rb'):
        """Modifies the mode in which the file is open.
        The file will be closed and re-opened in the new mode.

        """
        if not self._is_same_mode(mode):
            return self.__init__(self.uri, mode)
        return self

    def __init_raw_io(self, closefd=True, opener=None):
        """Initializes the underlying raw IO
        """
        # Create the underlying raw IO base on the scheme
        from . import gs, file
        if self.scheme == "file":
            logger.debug("Using local file: %s" % self.uri)
            return file.LocalFile(self.uri, self.mode, closefd, opener)
        elif self.scheme == "gs":
            logger.debug("Using GS file: %s" % self.uri)
            return gs.GSFile(self.uri, self.mode)
        raise NotImplementedError("No implementation available for scheme: %s" % self.scheme)

    def __init_io(self, buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        """Initializes the underlying IO
        """
        # The following code is modified based on python io.open()
        modes = set(self.mode)
        if modes - set("axrwb+t") or len(self.mode) > len(modes):
            raise ValueError("invalid mode: %r" % self.mode)

        creating = "x" in modes
        reading = "r" in modes
        writing = "w" in modes
        appending = "a" in modes
        updating = "+" in modes
        text = "t" in modes
        binary = "b" in modes

        if text and binary:
            raise ValueError("Can't have text and binary mode at once")
        if creating + reading + writing + appending > 1:
            raise ValueError("Can't have read/write/append mode at once")
        if not (creating or reading or writing or appending):
            raise ValueError("Must have exactly one of read/write/append mode")
        if binary and encoding is not None:
            raise ValueError("Binary mode doesn't take an encoding argument")
        if binary and errors is not None:
            raise ValueError("Binary mode doesn't take an errors argument")
        if binary and newline is not None:
            raise ValueError("Binary mode doesn't take a newline argument")
        if binary and buffering == 1:
            import warnings
            warnings.warn("line buffering (buffering=1) isn't supported in binary "
                          "mode, the default buffer size will be used",
                          RuntimeWarning, 2)

        self.raw_io = self.__init_raw_io(closefd, opener)
        opened_io = self.raw_io
        try:
            line_buffering = False
            if buffering == 1 or buffering < 0 and self.raw_io.isatty():
                buffering = -1
                line_buffering = True
            if buffering < 0:
                buffering = DEFAULT_BUFFER_SIZE
                # For local file only
                from .file import LocalFile
                if isinstance(self.raw_io, LocalFile):
                    try:
                        bs = os.fstat(self.raw_io.fileno()).st_blksize
                    except (OSError, AttributeError):
                        pass
                    else:
                        if bs > 1:
                            buffering = bs
            if buffering < 0:
                raise ValueError("invalid buffering size")
            if buffering == 0:
                if binary:
                    return self.raw_io
                raise ValueError("can't have unbuffered text I/O")
            if updating:
                buffered_io = BufferedRandom(self.raw_io, buffering)
            elif creating or writing or appending:
                buffered_io = BufferedWriter(self.raw_io, buffering)
            elif reading:
                buffered_io = BufferedReader(self.raw_io, buffering)
            else:
                raise ValueError("Unknown mode: %r" % self.mode)
            opened_io = buffered_io
            # Use TextIOWrapper for text mode
            if binary:
                return buffered_io
            text_io = TextIOWrapper(buffered_io, encoding, errors, newline, line_buffering)
            opened_io = text_io
            text_io.mode = self.mode
            return opened_io
        except Exception as ex:
            opened_io.close()
            raise ex

    def _is_same_mode(self, mode):
        """Checks if the mode is the same as the one in which the file is open
        """
        if mode:
            return sorted(self.mode) == sorted(mode)
        return True

    @staticmethod
    def init(uri, mode='rb'):
        """A factory method for initializing StorageIO()
        """
        return StorageFile(uri, mode)

    @staticmethod
    def load_json(uri):
        """Loads a json file into a dictionary.
        """
        return json.loads(StorageFile.init(uri).read())

    @property
    def size(self):
        """Size of the file in bytes.
        """
        return self.raw_io.size

    def exists(self):
        """Checks if the file exists.
        """
        return self.raw_io.exists()

    def open(self, mode=None):
        """Opens the file with mode.
        Generally there is no need to call open() after a StorageFile() is obtained.
        This is mainly for changing the mode in which the file is opened.
        """
        if mode and not self._is_same_mode(mode):
            self.__init__(self.uri, mode)
        return self

    def __enter__(self):
        # open will do nothing at this time.
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return

    def delete(self):
        """Deletes the file.

        Returns:

        """
        self.buffered_io.close()
        self.raw_io.delete()

    def copy(self, to):
        with self.open("rb") as f:
            with StorageFile(to, 'w+b') as f_to:
                return f_to.raw_io.load_from(f)

    def local(self):
        """Creates a temporary local copy of the file to improve the performance."""
        return self

    # The following methods calls the corresponding method in buffered_io
    def close(self):
        self.buffered_io.close()

    def readable(self):
        return self.buffered_io.readable()

    def read(self, size=None):
        return self.buffered_io.read(size)

    def read1(self, size=None):
        return self.buffered_io.read1(size)

    def readinto(self, b):
        return self.buffered_io.readinto(b)

    def readinto1(self, b):
        return self.buffered_io.readinto1(b)

    def writable(self):
        return self.buffered_io.writable()

    def write(self, b):
        return self.buffered_io.write(b)

    def truncate(self, pos=None):
        self.flush()
        if pos is None:
            pos = self.tell()
        return self.buffered_io.truncate(pos)

    def flush(self):
        return self.buffered_io.flush()

    def detach(self):
        return self.buffered_io.detach()

    def seekable(self):
        return self.buffered_io.seekable()

    def seek(self, pos, whence=SEEK_SET):
        return self.buffered_io.seek(pos, whence)

    def tell(self):
        return self.buffered_io.tell()

    def fileno(self):
        return self.buffered_io.fileno()

    def isatty(self):
        return self.buffered_io.isatty()

    @property
    def closed(self):
        return self.buffered_io.closed

    @property
    def raw(self):
        return self.raw_io

    @property
    def mode(self):
        return self._mode
