class DictList:
    def __init__(self, dict_list):
        self.dict_list = dict_list

    def unique_keys(self):
        keys = set()
        for d in dict_list:
            for key in self.dict_list.keys():
                keys.add(key)
        return keys

    def sort_by_value(self, key, reverse=False):
        return sorted(self.dict_list, key=lambda i : i.get(key))
