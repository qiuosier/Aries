class DictList(list):
    def unique_keys(self):
        keys = set()
        for d in self:
            for key in d.keys():
                keys.add(key)
        return keys

    def sort_by_value(self, key, reverse=False):
        return sorted(self, key=lambda i : i.get(key))
