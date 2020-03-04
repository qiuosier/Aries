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
        return sorted(self, key=lambda i : i.get(key))


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
    
    Returns:
        [type]: [description]
    """
    # Do nothing if the order_list if None or empty.
    if not order_list:
        return order_list, label_list
    order_list, label_list = (
        list(t) for t in zip(*sorted(zip(order_list, label_list), reverse=reverse))
    )
    return order_list, label_list
