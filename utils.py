import sys


def append_sys_path(folder_path):
    if folder_path not in sys.path:
        sys.path.append(folder_path)
