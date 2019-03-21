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


class StorageFolder(StorageObject):
    """Represents a storage folder.

    """
    def __init__(self, uri):
        super(StorageFolder, self).__init__(uri)
        # Make sure path ends with "/"
        if self.path and self.path[-1] != '/':
            self.path += '/'

    @property
    def files(self):
        """

        Returns: A list of StorageFile objects, each represents a file in this folder.

        """
        raise NotImplementedError

    @property
    def folders(self):
        """

        Returns: A list of StorageFolder objects, each represents a folder in this folder

        """
        raise NotImplementedError


class LocalFolder(StorageFolder):
    @property
    def files(self):
        return [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]

    @property
    def folders(self):
        return [f for f in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, f))]
