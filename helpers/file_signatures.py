"""Contains helper functions to create file signature dictionary from online resources.

Run the build_json() to create and save the signature dictionary.

File signature info are obtained from 
https://gist.github.com/qti3e/6341245314bf3513abb080677cd1c93b

The "signatures" variable in this module is a signature dictionary.
Each key is the number of offset bytes.
Each value is a dictionary with signatures(hex) as keys and file mine as values.

See Also: https://en.wikipedia.org/wiki/File_signature

"""
import json
import os
import requests


# File signature info are obtained from 
# https://gist.github.com/qti3e/6341245314bf3513abb080677cd1c93b
assets_dir = os.path.join(os.path.dirname(__file__), "..", "assets")


def update_signatures(signatures, v):
    """Updates the signature dictionary.
    
    Args:
        signatures (dict): Signature dictionary, which can be empty.
        v (dict): A dictionary, where the keys are:
            mime: the file mime type
            signs: a list of comma separated strings containing the offsets and the hex signatures.
    
    Returns:
        dict: The signature dictionary with signatures from v
    """
    mime = v.get("mime")
    # signs is a list of file signatures
    signs = v.get("signs")
    for sign in signs:
        # Each signature is a comma separated string
        # The number before the comma is the offset
        # The string after the comma is the signature hex.
        arr = sign.split(",")
        offset = arr[0]
        hex = arr[1]
        # Ignore 00 as it might appear in different files.
        if hex == "00":
            continue
        offset_signatures = signatures.get(offset, {})
        offset_signatures[hex] = mime
        signatures[offset] = offset_signatures
    return signatures


def load_signatures(signatures, file_path):
    """Load signatures from json file.
    
    Args:
        signatures (dict): Signature dictionary, which can be empty.
        file_path (str): The location of the json file containing a signature dictionary.
    
    Returns:
        dict: The update signature dictionary with signatures from the json file.
    """
    with open(file_path, 'r') as f:
        addon = json.load(f)
    for k, v in addon.items():
        offset_signatures = signatures.get(k, {})
        offset_signatures.update(v)
        signatures[k] = offset_signatures
    return signatures


def build_json():
    """Builds a json index file for identifying file types by file signatures.
    The json file stores the file signature dictionary
    The json file will be saved in the assets folder.
    """
    response = requests.get("https://gist.githubusercontent.com/qti3e/6341245314bf3513abb080677cd1c93b/raw/7d3a195305f4d2864d19bbf180e302579c820001/extensions.json")
    data = response.json()
    signatures = {}
    for v in data.values():
        signatures = update_signatures(signatures, v)
    
    # Load additional data
    signatures = load_signatures(signatures, os.path.join(assets_dir, "file_signatures_addon.json"))

    # Save data to json
    with open(os.path.join(assets_dir, "file_signatures.json"), 'w') as f:
        json.dump(signatures, f, indent=4)
