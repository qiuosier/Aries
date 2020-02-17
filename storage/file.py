import os
import shutil
import logging
from io import FileIO, SEEK_SET
from .base import StorageIOSeekable
from .io import StorageFolder
logger = logging.getLogger(__name__)


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
            logger.debug("Copying files from %s to %s" % (self.path, to))
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


class LocalFile(StorageIOSeekable):
    def __init__(self, uri, mode='r', closefd=True, opener=None):
        self.file_io = None
        self._closefd = closefd
        self._opener = opener
        StorageIOSeekable.__init__(self, uri, mode)

    # LocalFile supports low level API: fileno() and isatty()
    def fileno(self):
        return self.file_io.fileno()

    def isatty(self):
        return self.file_io.isatty()

    def tell(self):
        return self.file_io.tell()

    def seek(self, pos, whence=SEEK_SET):
        return self.file_io.seek(pos, whence)

    def seekable(self):
        return self.file_io.seekable()

    def read(self, size=None):
        self._check_readable()
        return self.file_io.read(size)

    def write(self, b):
        self._check_writable()
        n = self.file_io.write(b)
        return n

    def truncate(self, size=None):
        return self.file_io.truncate(size)

    def close(self):
        self._closed = True
        return self.file_io.close()

    @property
    def size(self):
        """File size in bytes"""
        if self.exists():
            return os.path.getsize(self.path)
        return None

    def exists(self):
        return True if os.path.exists(self.path) else False

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

    def open(self, mode=None):
        """
        """
        super().open(mode)
        if self.file_io:
            if not self._is_same_mode(mode):
                self.file_io.close()
                self.file_io = None
        if not self.file_io:
            mode = "".join([c for c in self.mode if c in "rw+ax"])
            self.file_io = FileIO(self.path, mode, closefd=self._closefd, opener=self._opener)
        return self
