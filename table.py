import csv


class TableRow(list):
    def __init__(self, headers, values):
        self.headers = headers
        self.upper_headers = [str(h).upper() for h in headers]
        super().__init__(values)

    def get(self, header, default=None, case_sensitive=True):
        if case_sensitive:
            col_index = self.headers.index(header)
        else:
            col_index = self.upper_headers.index(str(header).upper())
        if col_index < len(self):
            return self[col_index]
        else:
            return default

    def keys(self):
        return self.headers


class TableData:
    def __init__(self, headers, data):
        self.headers = headers
        self.data = data

    def get_value(self, row_index, header, default=None):
        col_index = self.headers.index(header)
        row = self.data[row_index]
        if col_index < len(row):
            return row[col_index]
        else:
            return default

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        if self.n < len(self.data):
            row = TableRow(self.headers, self.data[self.n])
            self.n += 1
            return row
        else:
            raise StopIteration

    @staticmethod
    def from_csv(file_path, header_row=0, **kwargs):
        headers = []
        data = []
        with open(file_path, 'r') as f:
            csv_file = csv.reader(f, **kwargs)
            for i, row in enumerate(csv_file):
                if i < header_row:
                    continue
                if i == header_row:
                    headers = row
                else:
                    data.append(row)
        return TableData(headers, data)

    @staticmethod
    def from_csv_string(s, header_row=0, **kwargs):
        """

        Args:
            s (str):
            header_row:
            **kwargs:

        Returns:

        """
        headers = []
        data = []
        csv_file = csv.reader(s.splitlines(), **kwargs)
        for i, row in enumerate(csv_file):
            if i < header_row:
                continue
            if i == header_row:
                headers = row
            else:
                data.append(row)
        return TableData(headers, data)
