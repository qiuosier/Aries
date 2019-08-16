import requests
import logging
from ..web import WebAPI
logger = logging.getLogger(__name__)


class GoogleDriveFile:
    def __init__(self, access_token, file_id):
        self.file_id = file_id
        self._metadata = None
        self.api = WebAPI("https://www.googleapis.com/")
        self.api.add_header(Authorization="Bearer %s" % access_token)

    @property
    def metadata(self):
        if not self._metadata:
            url = "https://www.googleapis.com/drive/v3/files/%s" % self.file_id
            self._metadata = self.api.get_json(url)
        return self._metadata

    @property
    def revisions(self):
        return self.api.get_json("https://www.googleapis.com/drive/v3/files/%s/revisions" % self.file_id)


class GoogleSheet(GoogleDriveFile):
    def get(self, **kwargs):
        return self.api.get_json(
            "https://sheets.googleapis.com/v4/spreadsheets/%s" % self.file_id,
            **kwargs
        )

    def get_data_grid(self, sheet_index=0, value_type="formattedValue"):
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

