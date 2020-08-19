import logging
import boto3
from botocore.exceptions import ClientError
from .base import StorageFolderBase
from .cloud import BucketStorageObject, CloudStoragePrefix, CloudStorageIO
logger = logging.getLogger(__name__)


class S3Object(BucketStorageObject):
    """
    See Also: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    """

    @property
    def key(self):
        return self.prefix

    @property
    def blob(self):
        """Gets or initialize a S3 Storage Object.

        Returns: An S3 storage object.

        This does not check whether the object exists.
        Use blob.exists() to determine whether or not the blob exists.

        """
        s3 = boto3.resource('s3')
        # logger.debug("Getting blob: %s" % self.uri)
        return s3.Object(self.bucket_name, self.prefix)

    def init_client(self):
        return boto3.client('s3')

    def init_bucket(self):
        s3 = boto3.resource('s3')
        logger.debug("Getting bucket %s" % self.bucket_name)
        return s3.Bucket(self.bucket_name)

    def exists(self):
        """

        Returns:

        See Also:
            https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html#accessing-a-bucket
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object
        """
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=self.prefix)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False
            raise e

    def create(self):
        return self.client.put_object(Bucket=self.bucket_name, Key=self.prefix)

    def delete(self):
        return self.blob.delete()


class S3Prefix(CloudStoragePrefix, S3Object):
    @property
    def uri_list(self):
        keys = []
        response = self.client.list_objects(Bucket=self.bucket_name, Prefix=self.prefix, Delimiter='')
        contents = response.get("Contents", [])
        keys.extend([element.get("Key") for element in contents])
        return [
            "s3://%s/%s" % (self.bucket_name, key)
            for key in keys
            if not key.endswith("/")
        ]

    def blobs(self, delimiter=""):
        return list(self.bucket.objects.filter(Prefix=self.prefix, Delimiter=delimiter))

    def exists(self):
        response = self.client.list_objects(Bucket=self.bucket_name, Prefix=self.prefix)
        contents = response.get("Contents", []) if response else None
        if contents:
            return True
        return False

    def delete(self):
        return self.bucket.objects.filter(Prefix=self.prefix).delete()


class S3Folder(S3Prefix, StorageFolderBase):
    def __init__(self, uri):
        """Initializes a Google Cloud Storage Directory.

        Args:
            uri: The path of the object, e.g. "gs://bucket_name/path/to/dir/".

        """
        # super() will call the __init__() of StorageObject, StorageFolder and GSObject
        S3Object.__init__(self, uri)
        StorageFolderBase.__init__(self, uri)

        # Make sure prefix ends with "/", otherwise it is not a "folder"
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"

    @property
    def folder_paths(self):
        """Folders(Directories) in the directory.
        """
        return self.__folders_paths()

    def __folders_paths(self):
        # TODO: Get the next page
        folders = []
        response = self.client.list_objects(Bucket=self.bucket_name, Prefix=self.prefix, Delimiter='/')
        prefixes = response.get("CommonPrefixes", [])
        folders.extend([p.get('Prefix') for p in prefixes if p.get('Prefix')])
        return [
            "s3://%s/%s" % (self.bucket_name, p)
            for p in folders
        ]

    @property
    def file_paths(self):
        """Files in the directory
        """
        paths = self.__file_paths()
        return paths

    def __file_paths(self):
        keys = []
        response = self.client.list_objects(Bucket=self.bucket_name, Prefix=self.prefix, Delimiter='/')
        contents = response.get("Contents", [])
        keys.extend([element.get("Key") for element in contents])
        return [
            "s3://%s/%s" % (self.bucket_name, key)
            for key in keys
            if not key.endswith("/")
        ]


class S3File(S3Object,CloudStorageIO):
    def __init__(self, uri):
        # file_io will be initialized by open()
        # self.file_io = None
        S3Object.__init__(self, uri)
        CloudStorageIO.__init__(self, uri)

    @property
    def updated_time(self):
        return self.blob.last_modified

    @property
    def md5_hex(self):
        e_tag = str(self.blob.e_tag)
        if len(e_tag) > 2:
            return e_tag.strip("\"")
        return None

    def get_size(self):
        return self.blob.content_length

    def upload(self, from_file_obj):
        self.blob.upload_fileobj(from_file_obj)

    def download(self, to_file_obj):
        self.blob.download_fileobj(to_file_obj)
        return to_file_obj

    def read_bytes(self, start, end):
        response = self.blob.get(Range="bytes=%s-%s" % (start, end))
        content = response.get("Body")
        if content:
            data = content.read()
            return data
        return ""

    def copy(self, to):
        dest = S3File(to)
        logger.debug("Creating copy of S3 file at %s" % to)
        return dest.blob.copy(dict(Bucket=self.bucket_name, Key=self.prefix))
