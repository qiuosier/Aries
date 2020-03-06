import contextlib
import requests
import logging
from urllib import request, error
from .base import StorageIOBase
from io import UnsupportedOperation
logger = logging.getLogger(__name__)


class WebFile(StorageIOBase):
    def __init__(self, uri):
        self.response = None
        StorageIOBase.__init__(self, uri)

    def open(self, mode='r', closefd=True, opener=None):
        super().open(mode)
        # Follows redirect if there is any.
        if self.scheme.startswith("http"):
            response = requests.head(self.uri, allow_redirects=True)
            url = response.url
        else:
            url = self.uri
        logger.debug("Reading data from %s" % url)
        self.response = request.urlopen(url)
        return self

    def close(self):
        self._closed = True
        if self.response:
            self.response.close()
        self.response = None

    def read(self, size=None):
        return self.response.read(size)

    def write(self, b):
        raise UnsupportedOperation("Writing to HTTP file is not supported.")

    def delete(self):
        raise UnsupportedOperation("Deleting HTTP file is not supported.")

    def exists(self):
        try:
            response = request.urlopen(self.uri)
            if response.getcode() == 200:
                response.close()
                return True
        except error.URLError:
            pass
        return False

    @property
    def updated_time(self):
        return None
