import logging
import time
from ..web import WebAPI
logger = logging.getLogger(__name__)


class GoogleDriveFile:
    def __init__(self, file_id, access_token=None, api_key=None):
        """Initializes the API for accessing Google Drive file.

        Args:
            file_id: Google Drive file ID. This can be obtained from the URL of the file.
            access_token: Google OAuth 2.0 access token
            api_key: Google API key

        Either access_token or api_key is required for accessing file on Google Drive.

        See Also:
            Using OAuth 2.0 to Access Google APIs
            https://developers.google.com/identity/protocols/oauth2
            Setting up API keys
            https://support.google.com/googleapi/answer/6158862?hl=en&ref_topic=7013279

        """
        self.file_id = file_id
        self._metadata = None
        if access_token:
            self.api = WebAPI("https://www.googleapis.com/")
            self.api.add_header(Authorization="Bearer %s" % access_token)
        elif api_key:
            self.api = WebAPI("https://www.googleapis.com/", key=api_key)
        else:
            raise ValueError("Either access_token or api_key is required to access Google Drive.")

    @property
    def metadata(self):
        """
        See Also: https://developers.google.com/drive/api/v3/fields-parameter
        """
        if not self._metadata:
            url = "https://www.googleapis.com/drive/v3/files/%s?fields=*" % self.file_id
            self._metadata = self.api.get_json(url)
        return self._metadata

    def get_meta(self, fields):
        """
        See Also: https://developers.google.com/drive/api/v3/fields-parameter
        """
        if isinstance(fields, list):
            fields = "&".join(["fields=%s" % f for f in fields])
            url = "https://www.googleapis.com/drive/v3/files/%s?%s" % (self.file_id, fields)
        else:
            url = "https://www.googleapis.com/drive/v3/files/%s?fields=%s" % (self.file_id, fields)
        return self.api.get_json(url)

    @property
    def revisions(self):
        return self.api.get_json("https://www.googleapis.com/drive/v3/files/%s/revisions" % self.file_id)


class GoogleSheet(GoogleDriveFile):
    """Represents a Google Sheet file
    """
    @property
    def sheets(self):
        """Gets a list of sheets from the API response (in terms of dictionaries).

        Returns:
            list: A list of dictionaries.
            Each dictionary contains sheet information as defined at
            https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets#Sheet
            This list is the value of the "sheets" key in the response of the response of
            GET https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}

        """
        return self.get().get("sheets")

    def get(self, **kwargs):
        """Returns properties of the spreadsheet along with properties and merges of sheets in the spreadsheet.
        By default, data within grids will not be returned. You can include grid data one of two ways:
            1. Specify a field mask listing your desired fields using the fields parameter.
            2. Use key word argument "includeGridData=True".
        If a field mask is set, the includeGridData parameter is ignored
        For large spreadsheets, it is recommended to retrieve only the specific fields of the spreadsheet that you want.

        To retrieve only subsets of the spreadsheet, use the ranges parameter. Multiple ranges can be specified.
        Limiting the range will return only the portions of the spreadsheet that intersect the requested ranges.
        Ranges are specified using A1 notation.

        Args:
            **kwargs: ranges, includeGridData, fields

        Returns: A dictionary containing the information of the spreadsheet.

        See Also:
            https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
            https://developers.google.com/sheets/api/guides/concepts#partial_responses
        """
        api_url = "https://sheets.googleapis.com/v4/spreadsheets/%s" % self.file_id
        response = self.api.get(api_url, **kwargs)
        counter = 0
        while response.status_code == 503:
            counter += 1
            if counter > 3:
                break
            time.sleep(3)
            response = self.api.get(api_url, **kwargs)
        return response.json()

    def values(self, data_range, **kwargs):
        """Gets the values of a specific range.

        Args:
            data_range: A1 notation of a range in a sheet.
                e.g. "SheetName!A1:B5"

        Returns: A dictionary containing the following keys:
            majorDimension:
            range: The actual range in the sheet containing data.
            values: Two 2-D list containing the values.
        """
        return self.api.get_json(
            "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s" % (self.file_id, data_range),
            **kwargs
        )

    def get_data_grid(self, sheet_index=0, value_type="formattedValue"):
        """Gets the values of a sheet

        Args:
            sheet_index: 0-based index of the sheet. Defaults to 0, the first sheet.
            value_type: The type of the values. Defaults to "formattedValue".
                This is a key of the CellData, as defined in
                https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
                Other string types include hyperlink and notes.

        Returns: A 2-D list of values. The type of the values depends on the value_type parameter.

        """
        sheets = self.get(includeGridData=True).get("sheets")
        if not sheets:
            return None

        data = sheets[sheet_index].get("data")
        if not data:
            return None

        rows = data[0].get("rowData", [])
        grid = []
        for row in rows:
            values = row.get("values", [])
            row_values = [v.get(value_type) for v in values]
            grid.append(row_values)
        return grid

    def get_row_data(self, row_number, sheet_name, from_col=None):
        """Gets the data values of a row from a sheet as a list

        Args:
            row_number (int): The 1-based row number.
            sheet_name (str): The name of the sheet.
            from_col (int): Gets the data starting from a certain column.
                This can be used to exclude the values header columns.
                All values of the rows will be returned if from_col is None, 0 or evaluated as False.

        Returns: A list of values.
            The value of an empty cell will be an empty string.


        """
        values = self.values("%s!%s:%s" % (sheet_name, row_number, row_number)).get("values")
        if not values:
            return []
        values = values[0]
        if from_col:
            values = values[from_col:] if from_col < len(values) else []
        return values

    def get_column_data(self, col_name, sheet_name, from_row=None):
        """Gets the data values of a column from a sheet as a list

        Args:
            sheet_name (str): The name of the sheet
            col_name (str): The column as letter string, e.g. "A" or "AK".
            from_row (int): Gets the data starting from a certain row.
                This can be used to exclude the values header rows.
                All values of the column will be returned if from_row is None, 0 or evaluated as False.

        Returns: A list of values.
            The value of an empty cell will be an empty string.


        """
        values = self.values("%s!%s:%s" % (sheet_name, col_name, col_name)).get("values")
        if not values:
            return []
        if from_row:
            values = values[from_row:]
        values = [v[0] if len(v) > 0 else "" for v in values]
        return values

    def append(self, data_range, rows):
        """Appends rows of values to the sheet after a "table" in the data_range.

        Args:
            data_range: The A1 notation of a range to search for a logical table of data.
                Values are appended after the last row of the table.
                For more details about how the table is detected, see:
                https://developers.google.com/sheets/api/guides/values#appending_values
            rows: A 2-D list containing the values to be appended.

        Returns:

        See Also:
            https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append

        """
        url = "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s:append?" \
              "valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS" % (self.file_id, data_range)
        data = {
            "range": data_range,
            "majorDimension": "ROWS",
            "values": rows
        }
        self.api.post_json(url, data)
