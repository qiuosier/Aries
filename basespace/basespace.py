"""Contains wrapper functions for BaseSpace API.
The "collection_name" parameter in this module is used to refer to the name of a certain type of objects in BaseSpace.
The following collection names are available for each BaseSpace account:
    runs,
    projects,
    appsessions,

For more details, see https://developer.basespace.illumina.com/docs/content/documentation/rest-api/data-model-overview
"""
import logging
import requests
from .utils import api_collection, api_response, build_api_url
logger = logging.getLogger(__name__)


def get_list(collection_name, match_key=None, match_value=None):
    """Gets a list of items in a BaseSpace collections.

    The returned result is a list of dictionaries.
    The match_key and match_value are used to filter the list of dictionaries..
    They work only if both match_key and match_value have values.
    When match_key and match_value are specified,
        the returned list will contain only matching dictionaries:
        1. A key named exactly as match_key,
        2. The value of the key matching the string in match_value.

    If the match_value does NOT end with "*", matching means exactly match of two strings.
    If the match_value ends with "*", matching means
        the value starts with a string of the match_value excluding the ending "*".

    Args:
        collection_name (str): "runs", "projects", or "appsessions".
        match_key (str): The key in the dictionary to be matched.
        match_value (str): The value in the dictionary to be matched.

    Returns: A list of items, each is dictionary.

    """
    items = api_collection("v1pre3/users/current/%s" % collection_name)
    result_list = []
    for item in items:
        if match_key is not None and match_value is not None:
            item_value = item.get(match_key)
            if str(match_value).endswith('*'):
                if not str(item_value).startswith(str(match_value)[:-1]):
                    continue
            elif item_value != match_value:
                continue
        # item_id = item.get("Id")
        # item_name = item.get("Name", "")
        # logger.debug("Getting %s...ID: %s - %s" % (collection_name, item_id, item_name))
        result_list.append(item)
    return result_list


def get_details(collection_name, basespace_id):
    """Gets the details of an object in a collection.

    Args:
        collection_name (str): "runs", "projects", or "appsessions".
        basespace_id (int): The BaseSpace ID of the object.

    Returns: A dictionary including the details of the object.

    """
    details = api_response("v1pre3/%s/%s" % (collection_name, basespace_id))
    return details


def get_property(collection_name, basespace_id, property_name):
    """Gets a property of an object in a collection

    Args:
        collection_name (str): "runs", "projects", or "appsessions".
        basespace_id (int): The BaseSpace ID of the object.
        property_name (str): The name of the property

    Returns: A dictionary including the details of the property. None if there is an error.

    """
    api_href = "v1pre3/%s/%s/properties/%s" % (collection_name, basespace_id, property_name)
    response = api_response(api_href)
    if not response:
        return None
    content = response.get("Content")
    if content:
        return content
    else:
        return response


def get_property_items(collection_name, basespace_id, property_name):
    """Gets a list of items in a property of an object in a collection.

    Args:
        collection_name (str): "runs", "projects", or "appsessions".
        basespace_id (int): The BaseSpace ID of the object.
        property_name (str): The name of the property

    Returns: A list of items, each is dictionary.

    """
    api_href = "v1pre3/%s/%s/properties/%s/items" % (collection_name, basespace_id, property_name)
    items = api_collection(api_href)
    if items:
        items = [item.get("Content") for item in items if item.get("Content")]
    else:
        items = []
    return items


def pack_sample_sheet(sample_sheet_lines):
    """Converts the data of a sample sheet from a list of string to a dictionary containing the sample data..

    Args:
        sample_sheet_lines (list): A list of strings, each is a line in the sample sheet.

    Returns: A dictionary containing the sample data, where,
        key = SampleId,
        value = A dictionary with "data_columns" as keys (UPPER CASE).

    """
    logger.debug("Packing sample sheet...%d lines", len(sample_sheet_lines))
    sample_sheet = dict()
    data_columns = [
        "SAMPLE_ID",
        "SAMPLE_NAME",
        "DESCRIPTION",
        "INDEX",
    ]
    column_idx = dict()
    data_row = False
    for sample_sheet_line in sample_sheet_lines:
        # line will be an array with the original data
        line = sample_sheet_line.strip("\r").split(",")
        # cells will be an array with all upper case data
        cells = [str(c).upper() for c in line]
        if not data_row and "[DATA]" in cells[0]:
            data_row = True
            continue
        # Skip rows before data row
        if not data_row:
            continue
        # Column names
        if 'SAMPLE_ID' in cells and 'SAMPLE_NAME' in cells:
            for i, col in enumerate(cells):
                if col in data_columns:
                    column_idx[col] = i
            continue
        # Skip the row without sample ID
        if cells[0] == '':
            continue
        # End of the data
        if data_row and "[" in cells[0] and "]" in cells[0]:
            break
        # Process sample data row
        sample_data = dict()
        for col in column_idx.keys():
            sample_data[col] = line[column_idx[col]]
        user_sample_id = sample_data.get("SAMPLE_ID")
        sample_sheet[user_sample_id] = sample_data
    return sample_sheet


def print_collection(collection):
    for item in collection:
        print("ID: %-15s | Name: %s" % (item.get("Id"), item.get("Name")))


def download_file(basespace_file_href, output_filename):
    url = build_api_url(basespace_file_href + "/content")
    response = requests.get(url, stream=True)
    with open(output_filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
