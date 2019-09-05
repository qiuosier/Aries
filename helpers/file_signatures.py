import json
import os
import requests


# File signature info are obtained from 
# https://gist.github.com/qti3e/6341245314bf3513abb080677cd1c93b
def build_json():
    """Builds a json index file for identifying file types by file signatures.
    The json file will be stored in the assets folder.
    """
    response = requests.get("https://gist.githubusercontent.com/qti3e/6341245314bf3513abb080677cd1c93b/raw/7d3a195305f4d2864d19bbf180e302579c820001/extensions.json")
    data = response.json()
    signatures = {}
    for v in data.values():
        mime = v.get("mime")
        signs = v.get("signs")
        for sign in signs:
            arr = sign.split(",")
            offset = arr[0]
            hex = arr[1]
            if hex == "00":
                continue
            offset_signatures = signatures.get(offset, {})
            offset_signatures[hex] = mime
            signatures[offset] = offset_signatures

    # Load additional data
    assets_dir = os.path.join(os.path.dirname(__file__), "..", "assets")
    file_path = os.path.join(assets_dir, "file_signatures_addon.json")
    with open(file_path, 'r') as f:
        addon = json.load(f)
    for k, v in addon.items():
        offset_signatures = signatures.get(k, {})
        offset_signatures.update(v)
        signatures[k] = offset_signatures

    # Save data to json
    file_path = os.path.join(assets_dir, "file_signatures.json")
    with open(file_path, 'w') as f:
        json.dump(signatures, f, indent=4)
