"""Contains functions for getting sample data from BaseSpace.
"""

import logging
from basespace.utils import api_collection, API_SERVER
logger = logging.getLogger(__name__)


def get_files(bs_sample_id, extension='fastq.gz'):
    """Gets a list of ('fastq.gz') files of a sample.

    Args:
        bs_sample_id: BaseSpace ID of a sample
        extension: Filter the files by file extension, default is set to 'fastq.gz'

    Returns: A list of file data, each is a dictionary.

    """
    if not bs_sample_id:
        return []
    files = api_collection("v1pre3/samples/%s/files" % bs_sample_id)
    if extension:
        files = [file for file in files if str(file.get("Name", "")).endswith(extension)]
    files = sorted(files, key=lambda f: f.get("Name"))
    return files


def get_fastq_pair(bs_sample_id):
    """Gets the BaseSpace urls for the pair of FASTQ files of a sample.

    Args:
        bs_sample_id: BaseSpace ID of a sample

    Returns: a tuple of URLs, (R1_URL, R2_URL).

    """
    fastq_r1 = None
    fastq_r2 = None
    files = get_files(bs_sample_id)
    for file in files:
        filename = file.get("Name")
        if "_R1_" in filename:
            href = file.get("Href")
            fastq_r1 = API_SERVER + href
        elif "_R2_" in filename:
            href = file.get("Href")
            fastq_r2 = API_SERVER + href
    return fastq_r1, fastq_r2
