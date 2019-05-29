"""Contains ExcelFile class for handling MS Excel file.

"""
import re
import logging
from openpyxl import load_workbook, Workbook
from openpyxl.writer.excel import save_virtual_workbook


logger = logging.getLogger(__name__)


class ExcelFile:
    """Represents an MS Excel file.
    """
    content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    def __init__(self, file_path=None, read_only=False):
        """Initializes an Excel file.

        Args:
            file_path: The path of the file.
            read_only: Indicates if the file should be opened in read-only mode.
        """
        self.filename = file_path
        if self.filename:
            self.workbook = load_workbook(
                filename=self.filename,
                guess_types=True,
                data_only=True,
                read_only=read_only
            )
        else:
            # Initialize new workbook if filename is not specified.
            self.workbook = Workbook()
        self.active_worksheet = self.workbook.active
        self.headers = self.get_row_values(0)

        self.column = dict()

    def set_worksheet(self):
        pass

    def set_headers(self, row_number):
        pass

    def save(self, file_path):
        self.workbook.save(file_path)

    def column_index(self, header, case_sensitive=False):
        """Gets the 1-based index of a column in the file.

        Args:
            header: The header (name) of the column.
            case_sensitive: Indicates whether the matching should be case sensitive.

        Returns: 1-based index of the column. Or -1 if the header is not found.

        """
        for i, value in enumerate(self.headers):
            if case_sensitive:
                if re.fullmatch(header, value):
                    return i + 1
            else:
                if re.fullmatch(header, value, re.IGNORECASE):
                    return i + 1
        return -1

    def read_active_worksheet(self, headers=None, read_only=False):
        """Gets the active worksheet in the file.

        Args:
            headers: A list of expected (not required) column names in the first row of the active sheet.
            read_only: Indicates if the file should be opened in read-only mode for large file.
            This function will save the column index to self.column[header],
                if the column has a matching header in headers.

        Returns: An openpyxl Worksheet object.

        """
        self.workbook = load_workbook(
            filename=self.filename,
            guess_types=True,
            data_only=True,
            read_only=read_only
        )
        self.active_worksheet = self.workbook.active
        self.headers = self.get_first_row_values()

        if headers is not None and headers:
            for header in headers:
                try:
                    idx = self.headers.index(header.upper())
                    self.column[header] = idx
                except ValueError:
                    logger.debug("Column not found: %s" % header)
        return self.active_worksheet

    def export_rows(self, row_list):
        """Exports specific rows from the file to a new workbook.

        Args:
            row_list: A list of row numbers.

        Returns: An openpyxl Workbook object.

        Remarks: Only the plain text are exported. Styles and formatting are not copied/exported.

        """
        in_ws = self.active_worksheet if self.active_worksheet else self.read_active_worksheet()
        out_wb = Workbook()
        out_ws = out_wb.active
        out_ws.append([cell.value for cell in in_ws[1]])
        for row in row_list:
            if row is None:
                continue
            cells = [cell.value if cell.value is not None else '' for cell in in_ws[int(row)]]
            out_ws.append(cells)
        return out_wb

    def get_first_row_values(self):
        """Gets the values of the first row in the active sheet as a list.
        All characters are converted the uppercase.

        """
        row = None
        for r in self.active_worksheet.rows:
            row = r
            break
        if row:
            row_values = [str(item.value).strip().upper() if item.value is not None else "" for item in row]
        else:
            logger.info("Failed to get the first row from the file.")
            row_values = None
        return row_values

    def get_row_values(self, row_number):
        """Gets the values of a row as a list of strings.
        Empty cell (None value) will be converted to empty string.
        The leading and trailing whitespaces of the cell value will be removed.

        Args:
            row_number: 0-based row number.

        Returns: A list of strings.
            The returned list will be empty if the row does not exist in the file.

        """
        row = None
        for i, r in enumerate(self.active_worksheet.rows):
            if row_number == i:
                row = r
                break
        if row:
            values = [str(item.value).strip() if item.value is not None else "" for item in row]
        else:
            logger.debug("The excel file does not have row %d" % row_number)
            values = []
        return values

    def has_headers(self, headers, flags=0):
        """Checks if the first row of the active worksheet has certain headers (a list of words).
        A header is considered found if it matches the value of a cell in the first row.
        The matching will be done using re.fullmatch(header, value, flags)
        The leading and trailing whitespaces of the cell value will be removed before matching.

        Args:
            headers: A list of strings, each is a header.
            flags: Flags for python regular expression matching.

        Returns: True if all strings in headers are found in the first row. Otherwise False.

        See Also:
            https://docs.python.org/3/library/re.html#re.fullmatch
            https://docs.python.org/3/library/re.html#re.compile

        """
        values = self.get_row_values(0)
        if values is None:
            logger.error("Failed to find the header row in the Excel file.")
            return False

        for header in headers:
            match = None
            for value in values:
                match = re.fullmatch(header, value, flags)
                if match is not None:
                    break
            if match is None:
                logger.error("Column \"%s\" not found in the Excel file." % header)
                logger.debug("Columns in the file: %s" % ",".join(values))
                return False
        return True

    def get_data_table(self, skip_first_row=True):
        """Reads the excel file into a 2D list in memory.
        This method may not be suitable for large file.

        Args:
            skip_first_row: Indicates whether the first row should be skipped.

        """
        if not self.active_worksheet:
            self.read_active_worksheet()

        table = []

        for index, row in enumerate(self.active_worksheet.rows):
            if index == 0 and skip_first_row:
                continue
            values = [item.value for item in row]
            table.append(values)
        return table
