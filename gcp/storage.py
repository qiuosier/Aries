import os
import io
import logging
from gcloud import storage
from gcloud.exceptions import InternalServerError
logger = logging.getLogger(__name__)
try:
    from ..storage import StorageObject, StorageFolder, StorageFile
    from ..tasks import FunctionTask
except (SystemError, ValueError):
    import sys
    from os.path import dirname
    aries_parent = dirname(dirname(dirname(__file__)))
    if aries_parent not in sys.path:
        sys.path.append(aries_parent)
    from Aries.storage import StorageObject, StorageFolder, StorageFile
    from Aries.tasks import FunctionTask


class GSObject(StorageObject):
    """The base class for Google Storage Object.

    Attributes:
        prefix: The Google Cloud Storage prefix, which is the path without the beginning "/"
    """
    def __init__(self, gs_path):
        """Initializes a Google Cloud Storage Object.

        Args:
            gs_path: The path of the object, e.g. "gs://bucket_name/path/to/file.txt".

        """
        super(GSObject, self).__init__(gs_path)
        self._client = storage.Client()
        self._bucket = None
        # The "prefix" for gcs does not include the beginning "/"
        if self.path.startswith("/"):
            self.prefix = self.path[1:]
        else:
            self.prefix = self.path

    def __getattribute__(self, item):
        
        return super(GSObject, self).__getattribute__(item)

    @property
    def bucket_name(self):
        """The name of the Google Cloud Storage bucket as a string."""
        return self.hostname

    def _get_client(self):
        self._client = storage.Client()

    @property
    def client(self):
        if not self._client:
            self._get_client()
        return self._client
            
    def _get_bucket(self):
        self._bucket = self.client.get_bucket(self.bucket_name)

    @property
    def bucket(self):
        if not self._bucket:
            self._get_bucket()
        return self._bucket

    @property
    def gs_path(self):
        return self.uri

    def blobs(self, delimiter=None):
        """Gets the blobs in the bucket having the prefix.

        The returning list will contain object in the folder and all sub-folders

        Args:
            delimiter: Use this to emulate hierarchy.
            If delimiter is None, the returning list will contain objects in the folder and in all sub-directories.
            Set delimiter to "/" to eliminate files in sub-directories.

        Returns: A list of GCS blobs.

        See Also: https://googleapis.github.io/google-cloud-python/latest/storage/blobs.html

        """
        return list(self.bucket.list_blobs(prefix=self.prefix, delimiter=delimiter))

    def delete(self):
        """Deletes all objects with the same prefix."""
        # This needs to be done before the batch.
        blobs = self.blobs()
        if blobs:
            with self.client.batch():
                for blob in blobs:
                    blob.delete()

    def copy(self, to):
        """Copies folder/file in a Google Cloud storage directory to another one.

        Args:
            to (str): Destination Google Cloud Storage path.
            If the path ends with "/", e.g. "gs://bucket_name/folder_name/",
                the folder/file will be copied under the destination folder with the original name.
            If the path does not end with "/", e.g. "gs://bucket_name/new_name",
                the folder/file will be copied and renamed to the "new_name".

        Returns: None

        Example:
            GSFolder("gs://bucket_a/a/b/c/").copy("gs://bucket_b/x/y/z") will copy the following files
                gs://bucket_a/a/b/c/d/example.txt
                gs://bucket_a/a/b/c/example.txt
            to
                gs://bucket_b/x/y/z/d/example.txt
                gs://bucket_b/x/y/z/example.txt

        """
        # Check if the destination is a bucket root.
        # Prefix will be empty if destination is bucket root.
        # Always append "/" to bucket root.
        if not GSObject(to).prefix and not to.endswith("/"):
            to += "/"

        if self.prefix.endswith("/"):
            # The source is a folder if its prefix ends with "/"
            if to.endswith("/"):
                # If the destination ends with "/",
                # copy the folder under the destination
                to += self.name + "/"
            else:
                # If the destination does not end with "/",
                # rename the folder.
                to += "/"
        else:
            # Otherwise, it can be a file or an object or a set of filtered objects.
            if to.endswith("/"):
                # If the destination ends with "/",
                # copy all objects under the destination
                to += self.name
            else:
                # If the destination does not end with "/",
                # simply replace the prefix.
                pass

        destination = GSObject(to)

        source_files = self.blobs()
        with self.client.batch():
            for blob in source_files:
                new_name = str(blob.name).replace(self.prefix, destination.prefix, 1)
                if new_name != str(blob.name):
                    self.bucket.copy_blob(blob, destination.bucket, new_name)

        logger.debug("%d blobs copied" % len(source_files))


class GSFolder(GSObject, StorageFolder):
    """Represents a Google Cloud Storage Folder

    Method Resolution Order: GSFolder, GSObject, StorageFolder, StorageObject
    """

    def __init__(self, gs_path):
        """Initializes a Google Cloud Storage Directory.

        Args:
            gs_path: The path of the object, e.g. "gs://bucket_name/path/to/dir/".

        """
        # super() will call the __init__() of StorageObject, StorageFolder and GSObject
        super(GSFolder, self).__init__(gs_path)

        # Make sure prefix ends with "/", otherwise it is not a "folder"
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"

    @property
    def folders(self):
        iterator = self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
        list(iterator)
        return [
            GSFolder("gs://%s/%s" % (self.bucket_name, p))
            for p in iterator.prefixes
        ]

    @property
    def files(self):
        return [
            GSFile("gs://%s/%s" % (self.bucket_name, b.name))
            for b in self.bucket.list_blobs(prefix=self.prefix, delimiter='/')
            if not b.name.endswith("/")
        ]


class GSFile(GSObject, StorageFile):
    def __init__(self, gs_path):
        """

        Args:
            gs_path:

        """
        # super() will call the __init__() of StorageObject, StorageFolder and GSObject
        super(GSFile, self).__init__(gs_path)

    @property
    def blob(self):
        """Gets the Google Cloud Storage Blob.

        Returns: A Google Cloud Storage Blob object.
            Use blob.exists() to determine whether or not the blob exists.

        """
        file_blob = self.bucket.get_blob(self.prefix)
        if file_blob is None:
            file_blob = self.bucket.blob(self.prefix)
        return file_blob

    def read(self):
        """Reads the file from the Google Cloud bucket to memory

        Returns: Bytes containing the entire contents of the file.
        """
        my_buffer = io.BytesIO()
        if self.blob.exists():
            self.blob.download_to_file(my_buffer)
            data = my_buffer.getvalue()
            my_buffer.close()
            return data
        else:
            return None

    def create(self):
        blob = storage.Blob(self.prefix, self.bucket)
        return blob

    def upload_from_file(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found: %s" % file_path)
        
        with open(file_path, 'rb') as f:
            self.blob.upload_from_file(f)
        return True
