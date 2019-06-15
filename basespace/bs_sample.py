"""Contains functions for getting sample data from BaseSpace.
"""

import logging
from .utils import api_collection, API_SERVER
from . import bs_project
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
    if files:
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
    if files:
        for file in files:
            filename = file.get("Name")
            if "_R1_" in filename:
                href = file.get("Href")
                fastq_r1 = API_SERVER + href
            elif "_R2_" in filename:
                href = file.get("Href")
                fastq_r2 = API_SERVER + href
    return fastq_r1, fastq_r2


def get_sample(project_name, sample_name):
    """Gets the information of a sample.

    Args:
        project_name (str): The name of the project.
        sample_name (str): The name of the sample (Sample ID).

    Returns: A dictionary containing the sample information.

    """
    samples = bs_project.get_samples(project_name)
    for sample in samples:
        if sample.get("Name") == sample_name:
            return sample

    return None


def get_files_by_name(project_name, sample_name):
    """Gets a list of files for a sample

    Args:
        project_name (str): The name of the project.
        sample_name (str): The name of the sample (Sample ID).

    Returns: A list of file information (dictionaries).

    """
    sample = get_sample(project_name, sample_name)
    if sample:
        href = sample.get("Href")
        api_url = href + "/files"
        return api_collection(api_url)
    return None
