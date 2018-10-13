import os
import logging
from gcloud import storage
logger = logging.getLogger(__name__)


def upload_file_to_bucket(local_file_path, cloud_file_path, bucket_name):
    """Uploads a file to Google cloud bucket.

    Args:
        local_file_path (str): The local file, including full file path and filename (source).
        cloud_file_path (str): The Google cloud storage path (destination).
        bucket_name (str): The name of the Google cloud bucket

    Returns: True if the operation is successful

    """
    if not os.path.exists(local_file_path):
        logger.error("Failed to upload file to Google cloud. File not found: " + local_file_path)
        return False
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(cloud_file_path)
    with open(local_file_path, 'rb') as my_file:
        logger.debug("Uploading file to gs://%s/%s" % (bucket_name, cloud_file_path))
        blob.upload_from_file(my_file)
    return True


def upload_file_to_bucket_and_delete(local_file_path, cloud_file_path, bucket_name):
    """Uploads a file to Google cloud bucket, and delete the file if the upload is success.

    Args:
        local_file_path (str): The local file, including full file path and filename (source).
        cloud_file_path (str): The Google cloud storage path (destination).
        bucket_name (str): The name of the Google cloud bucket

    Returns: True if the operation is successful

    """
    upload_success = upload_file_to_bucket(local_file_path, cloud_file_path, bucket_name)
    if upload_success and os.path.exists(local_file_path):
        os.remove(local_file_path)
        logger.debug("Removed %s" % local_file_path)
    return upload_success


def download_to_file(file_obj, cloud_file_path, bucket):
    """Downloads a file from the Google cloud bucket.

    Returns: True if the operation is successful

    """
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket)
    except Exception as ex:
        logger.error(str(ex))
        return False
    blob = bucket.blob(cloud_file_path)
    blob.download_to_file(file_obj)
    return True
