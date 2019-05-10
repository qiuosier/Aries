import logging
from . import basespace
logger = logging.getLogger(__name__)


def get_list(analysis_name=None):
    return basespace.get_list("appsessions", "Name", analysis_name)


def get_details(session_id):
    return basespace.get_details("appsessions", session_id)


def get_samples(session_id):
    return basespace.get_property_items("appsessions", session_id, "Output.Samples")


def get_input_run(session_id):
    runs = basespace.get_property_items("appsessions", session_id, "Input.Runs")
    if len(runs) > 0:
        return runs[0]
    else:
        return None


def get_sample_sheet(session_id):
    return basespace.get_property("appsessions", session_id, "Input.sample-sheet")


def get_sample_sheet_dict(session_id):
    sample_sheet_content = get_sample_sheet(session_id)
    if sample_sheet_content:
        lines = sample_sheet_content.split("\n")
        return basespace.pack_sample_sheet(lines)
    else:
        logger.debug("Sample sheet not found. Session ID: %s" % session_id)
        return None
