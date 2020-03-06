import os
import shutil
import logging
import datetime
from io import FileIO, SEEK_SET
from .base import StorageIOSeekable, StorageFolderBase
logger = logging.getLogger(__name__)


class LocalFolder(StorageFolderBase):
    def exists(self):
        return True if os.path.exists(self.path) else False

    @property
    def files(self):
        return self.file_paths

    @property
    def folders(self):
        return self.folder_paths

    @property
    def object_paths(self):
        return [os.path.join(self.path, f) for f in os.listdir(self.path)]

    @property
    def file_paths(self):
        return list(filter(lambda x: os.path.isfile(x), self.object_paths))

    @property
    def folder_paths(self):
        return list(filter(lambda x: os.path.isdir(x), self.object_paths))

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
        if to.endswith("/"):
            to += self.basename
        logger.debug("Copying files from %s to %s" % (self.path, to))
        # Copy the files recursively
        # copytree is not used here as it raises permission denied error in some python version.
        if not os.path.exists(to):
            os.makedirs(to)
        for file_path in self.file_paths:
            shutil.copy(file_path, to)
        for folder_path in self.folder_paths:
            LocalFolder(folder_path).copy(to)

    def delete(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)


class LocalFile(StorageIOSeekable):
    def __init__(self, uri):
        # file_io will be initialized by open()
        self.file_io = None
        StorageIOSeekable.__init__(self, uri)

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
        if self.file_io:
            self.file_io.close()
        self.file_io = None

    @property
    def size(self):
        """File size in bytes"""
        if self.exists():
            return os.path.getsize(self.path)
        return None

    @property
    def updated_time(self):
        return datetime.datetime.fromtimestamp(os.path.getmtime(self.path))

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

    def open(self, mode='r', closefd=True, opener=None):
        """
        """
        super().open(mode)
        if self.file_io:
            self.file_io.close()
            self.file_io = None
        if not self.file_io:
            mode = "".join([c for c in self.mode if c in "rw+ax"])
            self.file_io = FileIO(self.path, mode, closefd=closefd, opener=opener)
        return self
