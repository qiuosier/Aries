import os
import shutil
import logging
import datetime
import hashlib
from io import FileIO, SEEK_SET
from .base import StoragePrefixBase, StorageIOSeekable, StorageFolderBase
logger = logging.getLogger(__name__)


class LocalFolder(StorageFolderBase):
    def exists(self):
        logger.debug(self.path)
        return True if os.path.exists(self.path) else False

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

    def copy(self, to, contents_only=False):
        """Copies a folder and the files/folders in it.

        Args:
            to (str): The destination path.
            contents_only: Copies only the content of the folder.
                Defaults to False, i.e. a folder (with the same name as this folder)
                will be created at the destination to contain the files.
        """
        if not contents_only:
            to += self.basename
        logger.debug("Copying files from %s to %s" % (self.path, to))
        # Copy the files recursively
        # copytree is not used here as it raises permission denied error in some python versions.
        local_path = LocalFolder(to).path
        # Create the folder if it does not exist
        if not os.path.exists(local_path):
            logger.debug("Creating new folder: %s" % local_path)
            os.makedirs(local_path)
        for file_path in self.file_paths:
            logger.debug("Copying %s" % file_path)
            # Copy the file into the directory
            shutil.copy(file_path, local_path)

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
    def md5_hex(self):
        hash_md5 = hashlib.md5()
        with open(self.path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

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
        shutil.copyfile(self.path, LocalFile(to).path)

    def open(self, mode='r', closefd=True, opener=None):
        """
        """
        super().open(mode)
        if self.file_io:
            self.file_io.close()
            self.file_io = None
        if not self.file_io:
            mode = "".join([c for c in self.mode if c in "rw+ax"])
            # Create folder structure if one does not exists
            dir_name = os.path.dirname(self.path)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            self.file_io = FileIO(self.path, mode, closefd=closefd, opener=opener)
        return self


class LocalPrefix(StoragePrefixBase):
    @property
    def uri_list(self):
        uri_list = []
        dir_name = os.path.dirname(self.path)
        parent = LocalFolder(dir_name)
        obj_paths = [p for p in parent.object_paths if p.startswith(self.path)]
        for obj_path in obj_paths:
            if os.path.isfile(obj_path):
                uri_list.append(obj_path)
                continue
            if obj_path == self.path or obj_path in uri_list:
                continue
            if os.path.isdir(obj_path):
                if not obj_path.endswith("/"):
                    obj_path += "/"
                uri_list.extend(LocalPrefix(obj_path).uri_list)
                continue
        return uri_list
