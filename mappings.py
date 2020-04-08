from collections import abc
from copy import deepcopy


def flatten_values(obj):
    """Gets all values in a nested dictionary or list.

    Returns:

    """
    values = []
    if isinstance(obj, abc.Mapping):
        collection = obj.values()
    elif isinstance(obj, list):
        collection = obj
    else:
        return [str(obj)]

    for v in collection:
        if isinstance(v, abc.Mapping) or isinstance(v, list):
            values.extend(flatten_values(v))
        else:
            values.append(v)
    return values


def replace_values(obj, old_value, new_value):
    if issubclass(obj.__class__, dict):
        results = deepcopy(obj)
        for k, v in obj.items():
            results[k] = replace_values(v, old_value, new_value)
    elif issubclass(obj.__class__, list):
        results = [replace_values(v, old_value, new_value) for v in obj]
    else:
        results = str(obj).replace(old_value, new_value)
    return results
