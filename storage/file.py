import os
import shutil
import logging
from . import StorageFolder, StorageFile
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
