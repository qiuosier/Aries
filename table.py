"""Contains classes for reading table-like data.
"""
import csv
import logging
logger = logging.getLogger(__name__)


def list_index(values):
    return {values[i]: i for i in range(len(values))}


class TableRow(list):
    """Represents a row in a table as a list
    """
    def __init__(self, values, headers=None, index=None):
        """Initializes a list of values as a row in a table.

        Args:
            values (list): A list of values, usually strings.
            headers (list): Optional. A list as the header of the table.
            index (dict): Optional. A dictionary containing the mapping from the headers to the indices.
                key: a column header in headers
                value: 0-based index of header in headers.

            headers and index are used for accessing the value using the get(header) method.
            headers will be ignored if index is given.
            index will be generated from headers if it is not given.
        """
        if index is not None:
            self.headers = list(index.keys())
            self.index = index
        elif headers is not None:
            self.headers = headers
            self.index = list_index(headers)
        else:
            self.headers = []
            self.index = dict()
        self.upper_index = {str(k).upper(): v for k, v in self.index.items()}
        super().__init__(values)

    def get(self, header, default=None, case_sensitive=True):
        if case_sensitive:
            col_index = self.index.get(header)
        else:
            col_index = self.upper_index.get(str(header).upper())
        if col_index < len(self):
            return self[col_index]
        else:
            return default

    def keys(self):
        return self.headers


class TableData:
    """Represents a table-like data set, e.g. data from a CSV file.
    """
    def __init__(self, headers, data=None):
        """Initializes a table with headers and data.

        Args:
            headers: A list of strings as the headers of a table
            data: A 2-D list of the data in the table.
                data[i] is a list containing the data of the ith row.
        """
        self.headers = headers
        self.data = data
        self.header_index = list_index(headers)

    def get_header_index(self, header):
        """Gets the index of a header in the headers of the table.

        Args:
            header: one of the header in self.headers

        Returns: The 0-based index of the header, -1 if the header is not found.

        """
        return self.header_index.get(header, -1)

    def get_value(self, row_index, header, default=None):
        """Gets the value of a particular cell in the table.

        Args:
            row_index: 0-based row index
            header: The header of the column as it appears in self.headers
            default: The default value to be returned if the column is missing.

        Returns:

        """
        col_index = self.get_header_index(header)
        row = self.data[row_index]
        if col_index < len(row):
            return row[col_index]
        else:
            return default

    def __len__(self):
        """The number of rows in the table
        """
        return len(self.data)

    def __iter__(self):
        """Resets the row pointer to point to the first row.
        """
        self.row_pointer = 0
        return self

    def __next__(self):
        """Gets the next row of data from the table.

        Returns: A TableRow list.

        """
        if self.row_pointer < len(self.data):
            row = TableRow(self.data[self.row_pointer], self.headers)
            self.row_pointer += 1
            return row
        else:
            raise StopIteration

    @staticmethod
    def infer_csv_dialect(lines):
        logger.debug("Detecting dialect in CSV...")
        counter = 0

        # buffer = ""
        # for line in lines:
        #     buffer += line
        #     counter += 1
        #     if counter > 20:
        #         break
        # dialect = csv.Sniffer().sniff(buffer, '\t')

        for dialect in [csv.excel_tab, csv.excel]:
            delimiter_counts = dict()
            for line in lines:
                c = str(line).count(dialect.delimiter)
                if c == 0:
                    continue
                delimiter_counts[c] = delimiter_counts.get(c, 0) + 1
                counter += 1
                if counter > 20:
                    break
            if not delimiter_counts:
                continue
            if max(delimiter_counts.values()) > counter * 0.5:
                break
        logger.debug(repr("Detected delimiter: %s" % dialect.delimiter))
        return dialect

    @staticmethod
    def __parse_csv(lines, header_row=0, **kwargs):
        """Initializes a table from text lines in CSV format.

        Args:
            lines: A iterable object returning rows as strings in CSV format.
            header_row: The 0-based index of the header row in the file. Defaults to 0.
                rows before the header row will be ignored.
                values in the header row will be stored in self.header as a list.
                rows after the header row will be stored in self.data
            **kwargs: keyword arguments for python csv.reader()

        Returns: A TableData object.

        """
        headers = []
        data = []
        # Determine the delimiter if it is not given
        if "delimiter" not in kwargs and "dialect" not in kwargs:
            dialect = TableData.infer_csv_dialect(lines)
            kwargs["dialect"] = dialect
        csv_data = csv.reader(lines, **kwargs)
        for i, row in enumerate(csv_data):
            # Skip the rows before the header row
            if i < header_row:
                continue
            # Header row
            if i == header_row:
                headers = row
            else:
                # Data row
                data.append(row)
        return TableData(headers, data)

    @staticmethod
    def from_csv(file_path, header_row=0, **kwargs):
        """Initializes the table from a CSV file.

        Args:
            file_path: The full file path.
            header_row: The 0-based index of the header row in the file. Defaults to 0.
                rows before the header row will be ignored.
                values in the header row will be stored in self.header as a list.
                rows after the header row will be stored in self.data
            **kwargs: keyword arguments for python csv.reader()

        Returns: A TableData object.

        """
        # TODO: open file with URI
        with open(file_path, 'r') as f:
            return TableData.__parse_csv(f, header_row, **kwargs)

    @staticmethod
    def from_csv_string(s, header_row=0, **kwargs):
        """

        Args:
            s (str): A text string containing all rows of a CSV file.
            header_row: The 0-based index of the header row in the file. Defaults to 0.
                rows before the header row will be ignored.
                values in the header row will be stored in self.header as a list.
                rows after the header row will be stored in self.data
            **kwargs: keyword arguments for python csv.reader()

        Returns:

        """
        return TableData.__parse_csv(s.splitlines(), header_row, **kwargs)


class TableCSVFile(TableData):
    def __init__(self, file_path, header_row=0, **kwargs):
        self.file_path = file_path
        self.header_row = header_row
        self.kwargs = kwargs
        # headers and header_index will be initialized by TableData.__init___()
        self.headers = None
        self.header_index = None
        headers = self.__sniff()
        TableData.__init__(self, headers)

    def __sniff(self):
        """
        """
        with open(self.file_path, 'r') as f:
            # Determine the delimiter if it is not given
            if "delimiter" not in self.kwargs and "dialect" not in self.kwargs:
                dialect = TableData.infer_csv_dialect(f)
                self.kwargs["dialect"] = dialect
        # Find Header row
        for i, row in enumerate(self.rows):
            if i == self.header_row:
                return row

    def __len__(self):
        """The number of data rows in the table
        """
        return len(list(self.rows))

    def __iter__(self):
        """
        """
        return self.rows

    @property
    def rows(self):
        with open(self.file_path, 'r') as f:
            csv_data = csv.reader(f, **self.kwargs)
            for i, row in enumerate(csv_data):
                # Skip the rows before the header row
                if i < self.header_row:
                    continue
                yield TableRow(row, self.headers, index=self.header_index)
