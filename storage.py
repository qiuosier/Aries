import os
from urllib.parse import urlparse


class StorageObject:
    def __init__(self, uri):
        self.uri = str(uri)
        parse_result = urlparse(self.uri)
        self.scheme = parse_result.scheme
        self.hostname = parse_result.hostname
        self.path = parse_result.path

    def __str__(self):
        return self.uri


class StorageFolder(StorageObject):

    def __init__(self, uri):
        super(StorageObject, self).__init__(uri)
        # Make sure path ends with "/"
        # if self.path and self.path[-1] != '/':
        #     self.path += '/'

    @property
    def files(self):
        raise NotImplementedError

    @property
    def folders(self):
        raise NotImplementedError


class LocalFolder(StorageFolder):
    @property
    def files(self):
        return [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]

    @property
    def folders(self):
        return [f for f in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, f))]
