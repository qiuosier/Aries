"""Provides unified shortcuts/interfaces for access folders and files.
"""
import os
import json
import shutil
import logging
from io import RawIOBase
from urllib.parse import urlparse
logger = logging.getLogger(__file__)


class StorageObject:
    """Represents a storage object.
    This is the base class for storage folder and storage file.

    """
    def __init__(self, uri):
        """Initializes a storage object.

        Args:
            uri (str): Uniform Resource Identifier for the object.

        See https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
        """
        super(StorageObject, self).__init__()
        self.uri = str(uri)
        parse_result = urlparse(self.uri)
        self.scheme = parse_result.scheme
        if not self.scheme:
            self.scheme = 'file'
        self.hostname = parse_result.hostname
        self.path = parse_result.path

    def __str__(self):
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
        
        Returns:
            str: The basename of the file/folder
        """
        return self.basename


class StorageFile(StorageObject, RawIOBase):
    """Represents a storage file.
    Subclass should implement the RawIOBase interface, plus exists().
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
    def __init__(self, uri, mode='r'):
        super(StorageFile, self).__init__(uri)
        self.mode = 'r'
        self.__set_mode(mode)

    def __set_mode(self, mode):
        self.mode = mode
        logger.debug("File mode: %s" % self.mode)
        if 'x' in mode:
            if self.exists():
                raise FileExistsError("File %s already exists." % self.uri)
            self._writable = True
        elif 'a' in mode:
            self._writable = True
        elif 'w' in mode:
            self._writable = True
        else:
            self._writable = False

        if '+' in mode:
            self._writable = True

    def __call__(self, mode='r'):
        self.__set_mode(mode)
        return self

    @staticmethod
    def init(uri, mode='r'):
        """Opens a StorageFile as one of the subclass base on the URI.
        """
        if uri is None:
            raise ValueError("uri cannot be None")
        from .gcp.storage import GSFile
        uri = str(uri)
        if uri.startswith("/") or uri.startswith("file://"):
            logger.debug("Using local file: %s" % uri)
            return LocalFile(uri, mode)
        elif uri.startswith("gs://"):
            logger.debug("Using GS file: %s" % uri)
            return GSFile(uri, mode)
        logger.debug("No implementation available for scheme %s" % uri)
        return StorageFile(uri, mode)

    @staticmethod
    def load_json(uri):
        return json.loads(StorageFile.init(uri).read())

    def exists(self):
        raise NotImplementedError("exists() is not implemented for %s" % self.__class__.__name__)

    def open(self, mode=None):
        if mode:
            self.mode = mode

    def close(self):
        raise NotImplementedError("close() is not implemented for %s" % self.__class__.__name__)

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return

    def seekable(self):
        if self.exists():
            return True
        return False

    def readable(self):
        if self.exists():
            return True
        return False

    def writable(self):
        return self._writable

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
        from .gcp.storage import GSFolder
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


class LocalFile(StorageFile):
    def __init__(self, uri, mode='r'):
        super(LocalFile, self).__init__(uri, mode)
        self.file_obj = None
        self.__closed = True
        self.__offset = 0

    def delete(self):
        """Deletes the file if it exists.
        """
        if os.path.exists(self.path):
            os.remove(self.path)

    def copy(self, to):
        """Copies the file to another location.
        """
        # TODO: Copy file across different schema.
        if os.path.exists(self.path):
            shutil.copyfile(self.path, to)

    def exists(self):
        return True if os.path.exists(self.path) else False

    @property
    def size(self):
        """File size in bytes"""
        if self.exists():
            return os.path.getsize(self.path)

    def open(self, mode=None):
        """Opens the file for read/write in binary mode.
        Existing file will be overwritten.
        """
        super().open(mode)
        logger.debug("Opening %s with %s..." % (self.path, self.mode))
        self.file_obj = open(self.path, self.mode)
        self.__closed = False
        self.__offset = 0
        return self

    def close(self):
        """Flush and close the IO object.
        This method has no effect if the file is already closed.
        """
        if not self.__closed:
            try:
                logger.debug("Saving data into file %s" % self.path)
                self.flush()
            finally:
                self.__closed = True
        if self.file_obj:
            logger.debug("Closing file %s..." % self.path)
            self.file_obj.close()
            self.file_obj = None

    def seek(self, pos, whence=0):
        if self.file_obj:
            self.__offset = self.file_obj.seek(pos, whence)
        else:
            with open(self.path) as f:
                f.seek(self.__offset)
                self.__offset = f.seek(pos, whence)
        return self.__offset

    def read(self, size=None):
        if self.file_obj:
            b = self.file_obj.read(size)
            self.__offset = self.file_obj.tell()
        else:
            with open(self.path) as f:
                f.seek(self.__offset)
                b = f.read(size)
                self.__offset = f.tell()
        return b

    def writable(self):
        if not self.file_obj:
            return False
        if hasattr(self.file_obj, "writable"):
            return self.file_obj.writable()
        return True

    def write(self, b):
        """Writes data to the file. str will be encoded as bytes using default encoding.

        Args:
            b: str or bytes to be written into the file.

        Returns: The number of bytes written into the file.

        """
        if 'b' in self.mode and isinstance(b, str):
            b = b.encode()
        n = self.file_obj.write(b)
        self.__offset = self.file_obj.tell()
        return n

    def flush(self):
        if self.file_obj:
            logger.debug("Flusing...")
            return self.file_obj.flush()


class LocalFolder(StorageFolder):

    @property
    def files(self):
        return [LocalFile(f) for f in self.file_paths]

    @property
    def folders(self):
        return [LocalFolder(f) for f in self.folder_paths]

    @property
    def object_paths(self):
        return [os.path.join(self.path, f) for f in os.listdir(self.path)]

    @property
    def file_paths(self):
        return list(filter(lambda x: os.path.isfile(x), self.object_paths))

    @property
    def file_names(self):
        return [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]

    @property
    def folder_paths(self):
        return list(filter(lambda x: os.path.isdir(x), self.object_paths))

    @property
    def folder_names(self):
        return [f for f in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, f))]

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

    def exists(self):
        return True if os.path.exists(self.path) else False

    def create(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        return self

    def copy(self, to):
        """Copies a folder and the files/folders in it.
        
        Args:
            to (str): The destination path.
            If the path ends with "/", e.g. "/var/folder_name/",
                the folder will be copied UNDER the destination folder with the original name.
                e.g. "/var/folder_name/ORIGINAL_NAME"
            If the path does not end with "/", e.g. "/var/folder_name",
                the folder will be copied and renamed to "folder_name".
        """
        if os.path.isdir(self.path):
            if to.endswith("/"):
                to += self.basename
            shutil.copytree(self.path, to)

    def delete(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)
    
    def empty(self):
        for f in self.files:
            f.delete()
        for f in self.folders:
            f.delete()

    def is_empty(self):
        if self.files or self.folders:
            return False
        else:
            return True

    def filter_files(self, prefix):
        logger.debug("Filtering files by prefix: %s" % prefix)
        files = []
        for f in self.files:
            logger.debug(f.name)
            if f.name.startswith(prefix):
                files.append(f)
        return files
