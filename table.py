class TableData:
    def __init__(self, headers, data):
        self.headers = headers
        self.data = data

    def get_value(self, row_index, header):
        col_index = self.headers.index(header)
        return self.data[row_index][col_index]

    def __len__(self):
        return len(self.data)