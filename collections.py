from collections import abc
from copy import deepcopy


class DictList(list):
    """Represent a list of dictionaries
    """
    def unique_keys(self):
        """Gets the unique keys (union) of the dictionaries
        
        Returns:
            list: A list of keys from the dictionaries
        """
        keys = set()
        for d in self:
            for key in d.keys():
                keys.add(key)
        return keys

    def sort_by_value(self, key, reverse=False):
        """Sort the dictionaries by the value of a particular keys.
        
        Args:
            key: A key in the dictionaries.
            reverse (bool, optional): Reverse sorting. Defaults to False.
        
        Returns:
            list: A list of sorted dictionaries.
        """
        return sorted(self, key=lambda i: i.get(key), reverse=reverse)


class NestedList(list):
    """Represents a list of lists.

    """
    def __init__(self, *args):
        """Initializes a nested list.

        Args:
            *args: A list of lists.

        """
        super().__init__(args)

    def sort_elements(self, **kwargs):
        """Sorts the elements in each list based on the values of the first list in the nested list.
        When the values in the first list are the same, the values in the second list will be used.

        Args:
            **kwargs: keyword arguments for the built-in sorted() function.

        Returns: A sorted NestedList object.

        """
        if len(self) == 0:
            return []
        return NestedList(*[list(t) for t in zip(*sorted(zip(*self), **kwargs))])

    def flatten(self):
        """Flatten the lists into a single list
        """
        return [element for sub_list in self for element in sub_list]


def sort_lists(order_list, label_list, reverse=False):
    """Sort a two lists at the same time.
    The order will be determined by the order_list.

    Example:
        order_list = [2, 1, 3]
        label_list = ['A', 'B', 'C']
        After sorting,
        order_list = [1, 2, 3]
        label_list = ['B', 'A', 'C']
    
    Args:
        order_list (list): The list to be sorted.
        label_list (list): This list will be rearrange based on the order of order_list.
        reverse (bool, optional): Reverse sorting. Defaults to False.
    
    Returns: A 2-tuple of sorted (order_list, label_list).
    """
    # Do nothing if the order_list if None or empty.
    if not order_list:
        return order_list, label_list
    sorted_lists = NestedList(order_list, label_list).sort_elements(reverse=reverse)
    return sorted_lists[0], sorted_lists[1]


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
