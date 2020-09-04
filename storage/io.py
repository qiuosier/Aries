"""Provides unified IO and high level API for accessing folders and files.
"""
import os
import json
import logging
import binascii
import inspect
import traceback
from google.cloud import storage
from io import SEEK_SET, UnsupportedOperation
from io import BufferedIOBase, BufferedRandom, BufferedReader, BufferedWriter, TextIOWrapper, BytesIO
from .base import StorageObject, StorageFolderBase
from .cloud import CloudStorageIO
from . import gs, file, web, s3
logger = logging.getLogger(__name__)


class StoragePrefix(StorageObject):
    """Represents a collections of object with the same URI prefix
    """
    # Maps the scheme to the underlying raw class.
    registry = {
        "file": file.LocalPrefix,
        "gs": gs.GSPrefix,
        "s3": s3.S3Prefix
    }

    def __init__(self, uri):
        """Initializes a StoragePrefix object with a URI prefix.

        Args:
            uri: URI prefix.
        """
        super(StoragePrefix, self).__init__(uri)
        raw_class = self.registry.get(self.scheme)
        if not raw_class:
            raise NotImplementedError("No implementation available for scheme: %s" % self.scheme)
        self.raw = raw_class(uri)

    @property
    def objects(self):
        """StorageFile objects with the same prefix, including files in sub-folders.
        """
        return self.raw.objects

    @property
    def files(self):
        """StorageFile objects having the same prefix but not under any sub-folder after the prefix
        """
        if hasattr(self.raw, "files"):
            return self.raw.files
        # Use objects to determine files if the files property is not supported by the raw class
        return [f for f in self.objects if "/" not in f.path.replace(self.prefix, "")]

    @property
    def folders(self):
        """StorageFolder objects having the same prefix but not under any sub-folder after the prefix
        """
        if hasattr(self.raw, "folders"):
            return self.raw.folders
        folder_paths = set()
        for f in self.objects:
            relative_path = f.path.replace(self.prefix, "")
            if "/" in relative_path:
                dir_name = relative_path.split("/", 1)[0]
                folder_paths.add(self.prefix + dir_name)
        return [StorageFolder(p) for p in folder_paths]

    @property
    def size(self):
        return self.raw.size

    def is_file(self):
        """Determine if the object is a file.
        This will return False if the object does not exist or the object is a folder.
        """
        if self.path.endswith("/"):
            return False
        if not self.exists():
            return False
        return True

    def exists(self):
        return self.raw.exists()

    def delete(self):
        return self.raw.delete()

    def copy(self, to):
        """Copies all the files by replacing the prefix
        """
        logger.debug("Copying files from %s to %s" % (self.uri, to))
        dest = StorageObject(to)
        if dest.scheme == self.scheme:
            try:
                return self.raw.copy(to)
            except (AttributeError, UnsupportedOperation):
                pass

        for storage_file in self.objects:
            storage_file.copy(storage_file.uri.replace(self.uri, to))


class StorageFolder(StorageFolderBase):
    """Represents a storage folder.
    The StorageFolder class wraps an underlying raw class, which contains platform dependent implementation.
    The path attribute of a StorageFolder will always end with "/"

    """
    # Maps the scheme to the underlying raw class.
    registry = {
        "file": file.LocalFolder,
        "gs": gs.GSFolder,
        "s3": s3.S3Folder
    }

    def __init__(self, uri):
        """Initializes a StorageFolder.

        Args:
            uri: URI of the folder.
                The "file://" scheme will be used for local paths.
                A tailing slash "/" will be added to the path automatically.

        Examples:
            StorageFolder("gs://bucket_name/path/to/folder")
            StorageFolder("gs://bucket_name/path/to/folder/")
            StorageFolder("/path/to/local/folder")

        Raises:
            NotImplementedError: If there is no implementation for the scheme.

        """
        super(StorageFolder, self).__init__(uri)
        raw_class = self.registry.get(self.scheme)
        if not raw_class:
            raise NotImplementedError("No implementation available for scheme: %s" % self.scheme)
        self.raw = raw_class(uri)

    @staticmethod
    def init(uri):
        """Static method for initializing StorageFolder.
        """
        return StorageFolder(uri)

    @property
    def bucket_name(self):
        return self.__get_raw_attr("bucket_name", raise_error=True)

    @property
    def files(self):
        """

        Returns: A list of StorageFiles in the folder.

        """
        try:
            return self.raw.files
        except AttributeError:
            pass
        return [StorageFile(f) for f in self.file_paths]

    @property
    def folders(self):
        """

        Returns: A list of StorageFolders in the folder.

        """
        return [StorageFolder(f) for f in self.folder_paths]

    @property
    def objects(self):
        """Storage file objects in this folder and sub-folders.
        """
        try:
            return self.raw.objects
        except (AttributeError, UnsupportedOperation):
            pass

        object_list = self.files
        for folder in self.folders:
            object_list.extend(folder.objects)
        return object_list

    @property
    def size(self):
        try:
            return self.raw.size
        except (AttributeError, UnsupportedOperation):
            pass
        total = 0
        for f in self.files:
            s = f.size
            total += s if s else 0
        for folder in self.folders:
            s = folder.size
            total += s if s else 0
        return total

    @property
    def file_names(self):
        return [os.path.basename(f) for f in self.file_paths]

    @property
    def folder_names(self):
        return [str(f).rstrip("/").rsplit("/", 1)[-1] for f in self.folder_paths]

    @property
    def file_paths(self):
        return self.raw.file_paths

    @property
    def folder_paths(self):
        return self.raw.folder_paths

    def exists(self):
        """Checks if the folder exists.
        """
        return self.raw.exists()

    def create(self):
        """Creates a new folder.
        There will be no error if the folder already exists.
        """
        self.raw.create()
        return self

    def copy(self, to, contents_only=False):
        """Copies the folder.

        Args:
            to: The destination location,
            contents_only: Copies only the content of the folder.
                Defaults to False, i.e. a folder (with the same name as this folder)
                will be created at the destination to contain the files.

        Returns:

        """
        logger.debug("Copying files from %s to %s" % (self.uri, to))
        dest = StorageObject(to)
        if dest.scheme == self.scheme:
            try:
                return self.raw.copy(to, contents_only=contents_only)
            except (AttributeError, UnsupportedOperation):
                pass

        if not contents_only:
            to = os.path.join(to, self.name)
        # logger.debug(self.file_paths)
        for storage_file in self.files:
            storage_file.copy(os.path.join(to, storage_file.basename))
        # logger.debug(self.folder_paths)
        # Recursively copy the sub-folders.
        for storage_folder in self.folders:
            storage_folder.copy(os.path.join(to, storage_folder.basename))

    def download(self, local_path):
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        # Download the files using copy()
        for storage_file in self.files:
            storage_file.copy(os.path.join(local_path, storage_file.basename))
        # Recursively download the sub-folders.
        for storage_folder in self.folders:
            storage_folder.download(os.path.join(local_path, storage_folder.basename))

    def upload_from(self, local_path):
        raise NotImplementedError()

    def move(self, to, contents_only=False):
        """Moves the objects to another location."""
        self.copy(to, contents_only=contents_only)
        dest_folder = StorageFolder(to)
        if dest_folder.exists():
            # TODO: Check if the files are actually copied.
            if contents_only:
                self.empty()
            else:
                self.delete()
        else:
            raise FileNotFoundError("Failed to copy files to %s" % to)

    def delete(self):
        return self.raw.delete()

    def get_file_attributes(self, attribute=None):
        """Gets a list of files (represented by uri or other attribute) in the folder.

        Args:
            attribute: The attribute of the StorageFile to be returned in the list representing the files.

        Returns: A list of objects, each represents a file in this folder.
            If attribute is None, each object in the returning list will be the URI of the StorageFile
            If attribute is specified, each object in the returning list will be the attribute value of a StorageFile.

        """
        return self.get_attributes(self.files, attribute)

    def get_folder_attributes(self, attribute=None):
        """Gets a list of folders (represented by uri or other attribute) in the folder

        Args:
            attribute: The attribute of the StorageFolder to be returned in the list representing the folders.

        Returns: A list of objects, each represents a folder in this folder.
            If attribute is None, each object in the returning list will be the URI of the StorageFolder
            If attribute is specified, each object in the returning list will be the attribute value of a StorageFolder.

        """
        return self.get_attributes(self.folders, attribute)

    def get_folder(self, folder_name):
        """Gets a sub folder by name

        Args:
            folder_name (str): [description]

        Returns:
            LocalFolder: A LocalFolder instance of the sub folder.
                None if the sub folder does not exist.
        """
        for folder in self.folders:
            if folder.basename == folder_name:
                return folder
        return None

    def get_file(self, filename):
        for f in self.files:
            if f.basename == filename:
                return f
        return None

    def is_empty(self):
        if self.files or self.folders:
            return False
        else:
            return True

    def empty(self):
        for f in self.files:
            f.delete()
        for f in self.folders:
            f.delete()

    # Sub-class of StorageFolderBase can optionally implement the following methods
    #

    # def raw_if_exists(self, attr):
    #     from functools import wraps
    #
    #     @wraps(attr)
    #     def wrapper(*args, **kwargs):
    #         func_name = attr.__name__.rsplit(".", 1)[-1]
    #         if hasattr(self.raw, func_name):
    #             raw_attr = getattr(self.raw, func_name)(*args, **kwargs)
    #             if callable(raw_attr):
    #                 return raw_attr(*args, **kwargs)
    #             return raw_attr
    #         return attr(*args, **kwargs)
    #     return wrapper

    def __get_raw_attr(self, attr_name, raise_error=False):
        if hasattr(self.raw, attr_name):
            return getattr(self.raw, attr_name)
        if raise_error:
            raise AttributeError("%s:// does not support attribute %s" % (self.scheme, attr_name))
        return None

    def filter_files(self, prefix):
        attr_name = inspect.currentframe().f_code.co_name
        raw_attr = self.__get_raw_attr(attr_name)
        if raw_attr:
            return raw_attr(prefix)

        files = []
        for f in self.files:
            if f.name.startswith(prefix):
                files.append(f)
        logger.debug("%s files found with prefix %s" % (len(files), prefix))
        return files


class BufferedIOWrapper:
    """Delegates methods to buffered_io attribute.
    """
    def __init__(self):
        # Sub-class must define method to initialize buffered_io
        self.buffered_io = None

    def _check_closed(self):
        """Checks if the file is closed.

        Raises: ValueError if the file is closed.

        """
        if not self.buffered_io:
            raise ValueError(
                "I/O operation on closed file (buffer_io not initialized). "
                "Use open() or init() to open the file."
            )
        if self.buffered_io.closed:
            raise ValueError(
                "I/O operation on closed file (buffer_io closed). "
                "Use open() or init() to open the file."
            )

    def read1(self, size=None):
        self._check_closed()
        return self.buffered_io.read1(size)

    def readable(self):
        self._check_closed()
        return self.buffered_io.readable()

    def readinto(self, b):
        self._check_closed()
        return self.buffered_io.readinto(b)

    def readinto1(self, b):
        self._check_closed()
        return self.buffered_io.readinto1(b)

    def writable(self):
        self._check_closed()
        return self.buffered_io.writable()

    def write(self, b):
        self._check_closed()
        return self.buffered_io.write(b)

    def truncate(self, pos=None):
        self._check_closed()
        self.flush()
        if pos is None:
            pos = self.tell()
        return self.buffered_io.truncate(pos)

    def flush(self):
        self._check_closed()
        return self.buffered_io.flush()

    def detach(self):
        self._check_closed()
        return self.buffered_io.detach()

    def seekable(self):
        self._check_closed()
        return self.buffered_io.seekable()

    def seek(self, pos, whence=SEEK_SET):
        self._check_closed()
        return self.buffered_io.seek(pos, whence)

    def tell(self):
        self._check_closed()
        return self.buffered_io.tell()

    def fileno(self):
        self._check_closed()
        return self.buffered_io.fileno()

    def isatty(self):
        self._check_closed()
        return self.buffered_io.isatty()


class StorageFile(StorageObject, BufferedIOWrapper, BufferedIOBase):
    """Represents a storage file.

    Initializing a StorageFile object does not open the file.
    High level operations, e.g. copy(), delete() can be called without opening the file explicitly.
    read() is also supported without explicitly opening the file.
        If the file is closed, read() will open the file, read and return bytes, and then close the file.
        This is intended for one-time reading.

    To open a file for read/write, call open() explicitly.

    To initialize AND open a StorageFile, use StorageFile.init() static method.
    StorageFile.init() is designed be used in place of the python open()
    to obtain a file-like object for a file.
    See Also: https://docs.python.org/3/glossary.html#term-file-object

    To properly close the file, use context manager to with open() or init() when possible.

    Base on the scheme of the URI, StorageFile uses an sub-class of StorageIOBase as the underlying raw IO
    For binary mode, the raw IO is wrapped by BufferedIO
    For text mode, the bufferedIO is wrapped by TextIO

    Although StorageFile has raw_io as attribute, most operations should be done with the buffered_io

    Examples:
        Copy a file from local computer to Google storage:
        StorageFile("/path/to/file").copy("gs://path/to/file")

        Open a file and read all content:
        with StorageFile.init("/path/to/file") as f:
            content = f.read()

    Remarks:
        The order of the parent classes determines the method resolution order.
        See https://www.python.org/download/releases/2.3/mro/

    """

    registry = {
        "file": file.LocalFile,
        "gs": gs.GSFile,
        "s3": s3.S3File,
        "http": web.WebFile,
        "https": web.WebFile,
        "ftp": web.WebFile
    }

    def __init__(self, uri):
        """Initialize a StorageFile object.

        This will not open the file.
        However, high-level operations are supported without opening the file.

        Args:
            uri (str): The URI of the file, like file:///path/to/file or /path/to/file.
                If the URI does not contain a scheme, "file://" will be used.
        """
        StorageObject.__init__(self, uri)
        # Mode will be set by open()
        self._mode = None
        # The raw_io here is initialized but not opened.
        # The raw_io and buffered_io will be opened in __init_io()
        self.raw_io = self.__init_raw_io()
        # The buffered_io is the main IO stream used in the methods and properties.
        self.buffered_io = None

    def __call__(self, mode='r'):
        """Modifies the mode in which the file is open.
        The file will be closed and re-opened in the new mode.

        """
        if self.closed or not self._is_same_mode(mode):
            return self.open(mode)
        return self

    def __init_raw_io(self):
        """Initializes the underlying raw IO
        """
        # Create the underlying raw IO base on the scheme
        raw_class = self.registry.get(self.scheme)
        if not raw_class:
            raise NotImplementedError("No implementation available for scheme: %s" % self.scheme)
        return raw_class(self.uri)

    def __open_raw_io(self, closefd=True, opener=None):
        if not self.raw_io:
            self.__init_raw_io()

        # Raw IO always operates in binary mode, t and b will be ignored.
        mode = [c for c in self.mode if c in "rw+ax"]
        if self.scheme == "file":
            return self.raw_io.open(mode, closefd, opener)
        else:
            return self.raw_io.open(mode)

    def __buffer_size(self):
        """Determines the buffer size
        """

        buffering = self.BUFFER_SIZE
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
            raise ValueError("Invalid buffering size: %s" % buffering)
        return buffering

    @staticmethod
    def __validate_args(text, binary, creating, reading, writing, appending, encoding, errors, newline, buffering):
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

    def __init_buffer_io(self, buffering, binary, updating, creating, reading, writing, appending):
        if buffering < 0:
            buffering = self.__buffer_size()
        if buffering == 0:
            if binary:
                # logger.debug("Using raw_io as buffer_io for %s ..." % self.uri)
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
        # logger.debug("Initialized buffer IO for %s" % self.uri)
        return buffered_io

    def __init_io(self, buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        """Initializes the underlying raw IO and buffered IO
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

        self.__validate_args(text, binary, creating, reading, writing, appending, encoding, errors, newline, buffering)
        self.raw_io = self.__open_raw_io(closefd, opener)

        # Track the opened IO
        opened_io = self.raw_io
        try:
            line_buffering = False
            if buffering == 1 or buffering < 0 and self.raw_io.isatty():
                buffering = -1
                line_buffering = True
            opened_io = self.__init_buffer_io(buffering, binary, updating, creating, reading, writing, appending)

            # Use TextIOWrapper for text mode
            if binary:
                return opened_io
            text_io = TextIOWrapper(opened_io, encoding, errors, newline, line_buffering)
            opened_io = text_io
            text_io.mode = self.mode
            return opened_io
        except Exception as ex:
            # Close the opened IO if there is an error
            if opened_io:
                logger.debug("Closing opened_io...")
                opened_io.close()
            raise ex

    def _is_same_mode(self, mode):
        """Checks if the mode is the same as the one in which the file is open
        """
        if not self.mode:
            return False
        if mode:
            return sorted(self.mode) == sorted(mode)
        return True

    def open(self, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        """Opens a file and initializes the underlying IO.

        Args:
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

        Returns: self (StorageFile)

        """
        # Stores the arguments as protected attributes
        self._mode = str(mode)
        # logger.debug("Opening %s ..." % self.uri)
        # The raw_io will be initialized in __init_io()
        if not self.raw_io.closed:
            self.raw_io.close()

        # The buffered_io is the main IO stream used in the methods and properties.
        self.buffered_io = self.__init_io(buffering, encoding, errors, newline, closefd, opener)
        return self

    @staticmethod
    def init(uri, mode='r', **kwargs):
        """Initializes the StorageFile and open the underlying IO.
        This is a simplified version of StorageFile(uri).open(mode)

        Args:
            uri (str): The URI of the file, like file:///path/to/file or /path/to/file.
                If the URI does not contain a scheme, "file://" will be used.
            mode (str): The mode in which the file is opened,
                must be the combination of r, w, x, a, b, t and +

        Returns: A StorageFile object

        """
        storage_file = StorageFile(uri)
        storage_file.open(mode, **kwargs)
        return storage_file

    @staticmethod
    def load_json(uri, **kwargs):
        """Loads a json file into a dictionary.
        """
        return json.loads(StorageFile.init(uri, **kwargs).read())

    @property
    def closed(self):
        if not self.buffered_io:
            return True
        return self.buffered_io.closed

    @property
    def raw(self):
        """The underlying Raw IO
        """
        return self.raw_io

    @property
    def mode(self):
        return self._mode

    @property
    def size(self):
        """Size of the file in bytes.
        None will be returned if the size cannot be determined.
        """
        # hasattr will actually trigger the evaluation of the "size" property
        if hasattr(self.raw_io, "size"):
            try:
                return self.raw_io.size
            except Exception as ex:
                traceback.print_exc()
                logger.debug("Failed to get size: %s" % ex)
                pass
        logger.debug("%s has no size attribute." % self.raw_io.__class__.__name__)
        return None

    @property
    def md5_hex(self):
        """MD5 hex of the file content
        None will be returned if MD5 is not available.
        """
        return self.raw.md5_hex

    @property
    def blob(self):
        """The blob of the raw IO.

        Raises: AttributeError if blob is not supported.

        """
        if hasattr(self.raw_io, "blob"):
            return self.raw_io.blob
        raise AttributeError("%s:// does not support blob attribute" % self.scheme)

    @property
    def bucket_name(self):
        """Bucket name of the raw IO

        Raises: AttributeError if blob is not supported.

        """
        if hasattr(self.raw_io, "bucket_name"):
            return self.raw_io.bucket_name
        raise AttributeError("%s:// does not support bucket_name attribute" % self.scheme)

    @property
    def updated_time(self):
        return self.raw_io.updated_time

    def exists(self):
        """Checks if the file exists.
        This method may not return True until a new file is closed.
        """
        # TODO: File may not exist until new file is closed.
        return self.raw_io.exists()

    def __enter__(self):
        self._check_closed()
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.close()
        return

    def create(self):
        return self.raw_io.create()

    def delete(self):
        """Deletes the file.

        Returns:

        """
        if self.buffered_io:
            self.buffered_io.close()
        self.raw_io.delete()

    def copy(self, to):

        dest_file = StorageFile(to)
        # Use raw_io copy for same scheme, if possible
        if self.scheme == dest_file.scheme and hasattr(self.raw_io, "copy"):
            return self.raw_io.copy(to)
        logger.debug("Copying file stream to %s" % to)
        with self.open("rb") as f:
            with dest_file.open('wb') as f_to:
                return self.copy_stream(f, f_to)

    def move(self, to):
        """Moves the objects to another location."""
        self.copy(to)
        dest_file = StorageFile(to)
        if dest_file.exists():
            self.delete()
        else:
            raise FileNotFoundError("Failed to copy files to %s" % to)

    def local(self):
        """Creates a temporary local copy of the file to improve the performance.
        This requires the supports from the underlying raw IO.
        """
        try:
            self.raw_io.local()
        except (AttributeError, UnsupportedOperation):
            pass
        return self

    def download(self, to_file_obj=None, **kwargs):
        """Downloads the file to a file-object.

        Args:
            to_file_obj: A file-object. A NamedTemporaryFile will be created if this is None

        Returns: The file-object.

        """
        if not self.raw_io.exists():
            raise FileNotFoundError("File %s not found." % self.uri)

        # Create a new temp file if to_file_obj is None
        if not to_file_obj:
            to_file_obj = self.create_temp_file(**kwargs)
        try:
            logger.debug("Downloading %s ..." % self.uri)
            self.raw_io.download(to_file_obj)
            to_file_obj.flush()
            return to_file_obj
        except (AttributeError, UnsupportedOperation):
            pass
        # Copy the stream
        with self.open("rb") as f:
            self.copy_stream(f, to_file_obj)
        return to_file_obj

    def upload(self, from_file_obj):
        try:
            return self.raw_io.upload(from_file_obj)
        except (AttributeError, UnsupportedOperation):
            pass
        if not self.closed:
            self.buffered_io.close()
        with self.open("wb") as f:
            f.raw_io.load_from(from_file_obj)

    def upload_from_file(self, file_path):
        with open(file_path, 'rb') as f:
            self.upload(f)

    def is_gz(self):
        """Determine if the file is gz compressed.
        """
        # Reset the offset to the beginning of the file if file is opened.
        if self.closed:
            with self.open("rb") as f:
                b = f.read(2)
        else:
            offset = self.tell()
            self.seek(0)
            b = self.read(2)
            # Move offset back
            self.seek(offset)
        b = binascii.hexlify(b)
        logger.debug("File begins with: %s" % b)
        return b == b'1f8b'

    def close(self):
        # logger.debug("Closing %s ..." % self.uri)
        results = None
        if self.buffered_io:
            # buffered_io will close raw_io
            results = self.buffered_io.close()
            # Remove the buffered_io reference so that it will close the raw IO
            self.buffered_io = None
        elif self.raw_io and not self.raw_io.closed:
            self.raw_io.close()
        return results

    def read(self, size=None):
        """Reads bytes from the file

        Args:
            size: The maximum number of bytes to be returned.
                Less bytes will be returned if the file size is less than the size argument.

        Returns: Bytes content from the file.

        As a shortcut, read() can be called without initializing buffered_io

        """
        # As a shortcut, read() can be called without initializing buffered_io
        if self.closed:
            # The returned content will always be bytes in this case.
            with self.raw_io.open('rb') as f:
                return f.read(size)
        return self.buffered_io.read(size)

    def write_string(self, s, encoding="utf-8", errors="strict"):
        """Writes a string into the file.

        Args:
            s (str): String to be written into the file
            encoding: Same as encoding in str.encode()
            errors: Same as errors in str.encode()

        """
        if not s:
            return
        b = s.encode(encoding, errors)
        if issubclass(self.raw_io.__class__, CloudStorageIO):
            with BytesIO() as f:
                f.write(b)
                f.seek(0)
                self.raw_io.upload(f)
        elif self.closed:
            with self.raw_io.open('wb') as f:
                f.write(b)
        elif 'b' in self.mode:
            self.write(b)
        else:
            self.write(s)
        return self


class FileBatch(list):

    BATCH_SIZE = 900

    @property
    def scheme(self):
        if not self:
            return None
        return self[0].scheme

    def append(self, obj):
        if not isinstance(obj, StorageFile):
            raise ValueError(
                "Only StorageFile object can be appended to FileBatch. (obj: %s)"
                % type(obj)
            )
        if self and self.scheme != obj.scheme:
            raise ValueError("Objects in BatchFiles must have the same scheme. (obj: %s)" % obj.scheme)
        super().append(obj)

    def delete(self):
        blob_count = len(self)
        # logger.debug("Deleting %s files.." % blob_count)
        i = 0
        while i < len(self):
            end = i + self.BATCH_SIZE
            if end > len(self):
                end = len(self)
            batch = self[i:end]
            i = end
            if self.scheme == "gs" and batch:
                self.delete_gs_batch(batch)
            elif self.scheme == "s3" and batch:
                self.delete_s3_batch(batch)
            else:
                raise UnsupportedOperation("Scheme %s is not supported." % self.scheme)
        logger.debug("Deleted %s files." % blob_count)

    @staticmethod
    def delete_gs_batch(batch):
        if not len(batch):
            return

        client = storage.Client()

        with client.batch():
            for f in batch:
                f.blob.delete()

    @staticmethod
    def delete_s3_batch(batch):
        bucket = batch[0].raw_io.bucket
        bucket.delete_objects(Delete=dict(Objects=[{"Key": f.prefix} for f in batch], Quiet=True))
