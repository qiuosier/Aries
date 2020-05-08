import sys


def append_sys_path(folder_path):
    """Appends a path to sys.path
    """
    if folder_path not in sys.path:
        sys.path.append(folder_path)


def group_by_attr(records, attr):
    """Groups a list of objects by the value of an attribute

    Returns: A dictionary, where
        The keys are the distinct values of the given attribute.
        Each value is a list of objects with the same attribute value.
    """
    groups = dict()
    for record in records:
        key = getattr(record, attr)
        group = groups.get(key, [])
        group.append(record)
        groups[key] = group
    return groups
