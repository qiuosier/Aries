"""Contains functions for getting run data from BaseSpace.
"""

import logging
import string
import random
import os
import csv
import tempfile
from .utils import api_collection, api_response
from . import basespace, bs_sample
logger = logging.getLogger(__name__)


def get_list(experiment_name=None):
    return basespace.get_list("runs", "ExperimentName", experiment_name)


def get_details(run_id):
    details = api_response("v1pre3/runs/%s" % run_id)
    return details


def get_samples(run_id):
    """

    This function ignores the "undetermined" samples.
    Samples with the same SampleId and same FASTQ files (same file size and same number of reads) are merged.
        Only the latest one will be returned.

    Args:
        run_id:

    Returns:

    """
    # Use a dictionary to store the samples and remove the "duplicates"
    # The key of the dictionary is "[SampleId]_[NumReadsRaw]_[FileSize1]_[FileSize2]"
    samples_dict = dict()
    samples = basespace.get_property_items("runs", run_id, "Output.Samples")
    # The response is a list of dictionaries.
    # Each dictionary has two keys: "Content" and "Id"
    for sample in samples:
        user_sample_id = sample.get("SampleId")
        if not user_sample_id or str(user_sample_id).startswith("Undetermined"):
            continue
        bs_sample_id = sample.get("Id")
        num_read = sample.get("NumReadsRaw")
        date_created = sample.get("DateCreated")
        if not date_created:
            logger.warning("DateCreated field of sample %s is empty." % user_sample_id)
        files = bs_sample.get_files(bs_sample_id)
        key = "%s_%s_%s" % (user_sample_id, num_read, "_".join([str(file.get("Size")) for file in files]))
        if not samples_dict.get(key) or date_created > samples_dict[key].get("DateCreated"):
            sample.update({"files": files})
            samples_dict[key] = sample
    return list(samples_dict.values())


def get_sample_sheet_href(run_id):
    sample_sheet = None
    files = api_collection("v1pre3/runs/%s/files" % run_id)
    for file in files:
        name = file.get("Name")
        if 'SampleSheet' in name and 'csv' in name:
            sample_sheet = file.get("Href")
    if not sample_sheet:
        logger.error("Sample Sheet not found for Run %s" % run_id)
    return sample_sheet


def get_sample_sheet_dict(run_id):
    basespace_href = get_sample_sheet_href(run_id)
    if not basespace_href:
        return None

    filename = "SampleSheet_%s.csv" % ''.join(random.choice(string.ascii_uppercase) for _ in range(6))
    temp_file = os.path.join(tempfile.gettempdir(), filename)
    basespace.download_file(basespace_href, temp_file)
    sample_sheet = None
    # Get data from sample sheet
    if os.path.exists(temp_file):
        with open(temp_file) as csv_file:
            lines = csv.reader(csv_file)
            sample_sheet = basespace.pack_sample_sheet(lines)
        os.remove(temp_file)
    return sample_sheet
