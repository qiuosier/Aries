import os
from urllib.parse import urlparse


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
        self.hostname = parse_result.hostname
        self.path = parse_result.path

    def __str__(self):
        return self.uri

    @property
    def basename(self):
        return os.path.basename(self.path)


class StorageFile(StorageObject):
    """Represents a storage file.

    """
    def __init__(self, uri):
        super(StorageFile, self).__init__(uri)


class StorageFolder(StorageObject):
    """Represents a storage folder.

    """
    def __init__(self, uri):
        super(StorageFolder, self).__init__(uri)
        # Make sure path ends with "/"
        if self.path and self.path[-1] != '/':
            self.path += '/'

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
        """Gets a list of files in the folder.

        Args:
            attribute: The attribute of the StorageFile to be returned in the list representing the files.

        Returns: A list of objects, each represents a file in this folder.
            If attribute is None, each object in the returning list will be the URI of the StorageFile
            If attribute is specified, each object in the returning list will be the attribute value of a StorageFile.

        """
        return self._get_attribute(self.files, attribute)

    def get_folders(self, attribute=None):
        """Gets a list of folders in the folder

        Args:
            attribute: The attribute of the StorageFolder to be returned in the list representing the folders.

        Returns: A list of objects, each represents a folder in this folder.
            If attribute is None, each object in the returning list will be the URI of the StorageFolder
            If attribute is specified, each object in the returning list will be the attribute value of a StorageFolder.

        """
        return self._get_attribute(self.folders, attribute)


class LocalFile(StorageFile):
    pass


class LocalFolder(StorageFolder):
    @property
    def file_paths(self):
        return [
            os.path.join(self.path, f)
            for f in os.listdir(self.path)
            if os.path.isfile(os.path.join(self.path, f))
        ]

    @property
    def file_names(self):
        return [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]

    @property
    def folder_paths(self):
        return [
            os.path.join(self.path, f)
            for f in os.listdir(self.path)
            if os.path.isdir(os.path.join(self.path, f))
        ]

    @property
    def folder_names(self):
        return [f for f in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, f))]

    @property
    def files(self):
        return [LocalFile(f) for f in self.file_paths]

    @property
    def folders(self):
        return [LocalFolder(f) for f in self.folder_paths]
