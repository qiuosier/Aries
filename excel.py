"""Handles the operation of MS Excel file.

"""
from openpyxl import load_workbook, Workbook
from openpyxl.writer.excel import save_virtual_workbook
import logging


logger = logging.getLogger('commons')


class ExcelFile(object):
    """Represents an MS Excel file.

    """
    content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    def __init__(self, file_path):
        """Initializes the object with a filename.

        Args:
            file_path: The path of the file.
        """
        self.filename = file_path
        self.active_worksheet = None
        self.column = dict()
        self.headers = None
        self.workbook = None

    def save(self, file_path):
        self.workbook.save(file_path)

    def column_index(self, header, case_sensitive=False):
        for i, h in enumerate(self.headers):
            if case_sensitive:
                if str(h) == str(header):
                    return i + 1
            else:
                if str(h).upper() == str(header).upper():
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
        if not self.active_worksheet:
            self.read_active_worksheet()

        row = None
        for r in self.active_worksheet.rows:
            row = r
            break
        if row:
            row_values = [str(item.value).strip(" ").upper() if item.value is not None else "" for item in row]
        else:
            logger.info("Failed to get the first row from the file.")
            row_values = None
        return row_values

    def has_headers(self, headers):
        """Checks if the first row of the file has all the required column names.

        Args:
            headers: A list of strings, each is the name of a required column.

        Returns: True if all strings in headers are found. Otherwise False.

        """
        row_values = self.get_first_row_values()
        if row_values is None:
            logger.error("Failed to find the header row in the Excel file.")
            return False

        for header in headers:
            if header.upper() not in row_values:
                logger.error("Column %s not found in the Excel file." % header)
                logger.debug(row_values)
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
