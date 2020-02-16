"""Provides unified high level IO interface for accessing folders and files.
"""
import os
import json
import logging
from io import BufferedRandom, FileIO, SEEK_SET, DEFAULT_BUFFER_SIZE
from . import StorageObject
from . import gs, file
logger = logging.getLogger(__name__)


class StorageFile(StorageObject, BufferedRandom):
    """Represents a storage file.

    The storage.init() is designed be used in place of the python open()
    to obtain a file-like object for a file.
    See Also: https://docs.python.org/3/glossary.html#term-file-object

    https://github.com/python/cpython/blob/1ed61617a4a6632905ad6a0b440cd2cafb8b6414/Lib/_pyio.py#L1387

    A subclass should implement the RawIOBase interface.
    See Also: https://docs.python.org/3/library/io.html#class-hierarchy


    In addition, a subclass should also implement:
        exists(), to determine whether the file exists.
        local(), to create a local copy of the file for fast access.

    For subclass representing writable object, implement:
        delete(), to delete the file.
        copy_from(), to copy a file from a file-like object (stream).


    This class implements seekable() and readable() to return True if the file exists.

    The following should be implemented:
    For seeking:
        seek(self, pos, whence=0)
        seekable()

    For reading:
        read()
        readable()

    For writing:
        write(b)
        writable()
        flush()

    """

    def __init__(self, uri, mode='rb'):
        self.mode = mode
        StorageObject.__init__(self, uri)
        if self.scheme == "file":
            logger.debug("Using local file: %s" % uri)
            self.raw = FileIO(self.path, mode)
        elif self.scheme == "gs":
            logger.debug("Using GS file: %s" % uri)
            self.raw = gs.GSFile(uri, mode)
        else:
            raise NotImplementedError("No implementation available for scheme %s" % uri)
        BufferedRandom.__init__(self, self.raw)

    def __call__(self, mode='rb'):
        if self.mode != mode:
            return self.init(self.uri, mode)
        return self

    @staticmethod
    def init(uri, mode='rb'):
        """Opens a StorageFile as one of the subclass base on the URI.
        """
        return StorageFile(uri, mode)

    @staticmethod
    def load_json(uri):
        return json.loads(StorageFile.init(uri).read())

    def exists(self):
        return self.raw.exists()

    def open(self, mode=None):
        if mode and self.mode != mode:
            self.__init__(self.uri, mode)
        return self

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return

    def copy(self, to):
        raise NotImplementedError("copy() is not implemented for %s" % self.__class__.__name__)

    def local(self):
        """Creates a temporary local copy of the file to improve the performance."""
        return self


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
        from . import LocalFolder, GSFolder
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
