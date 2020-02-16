import os
import shutil
import logging
from io import FileIO
from . import StorageFolder, StorageIOSeekable
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


class LocalFile(FileIO, StorageIOSeekable):
    def __init__(self, uri, mode='r'):
        StorageIOSeekable.__init__(self, uri, mode)
        FileIO.__init__(self, self.path, self.mode)

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
        """
        """
        super().open(mode)
        FileIO.__init__(self, self.path, self.mode)
        return self
