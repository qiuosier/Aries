import sys


def get_unique_keys(dict_list):
    keys = set()
    for d in dict_list:
        for key in d.keys():
            keys.add(key)
    return keys


def append_sys_path(folder_path):
    if folder_path not in sys.path:
        sys.path.append(folder_path)