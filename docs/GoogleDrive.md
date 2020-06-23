# Accessing Files on Google Drive
The `Aries.Google.drive` module provides simple access to file on Google Drive. At this moment, it is mainly designed for accessing data on Google Sheet.

[![PyPI version](https://badge.fury.io/py/Aries-storage.svg)](https://pypi.org/project/Aries-storage/)

Google provides [Python API](https://developers.google.com/drive/api/v3/quickstart/python) for accessing Google Drive, including Google Sheets. The `Aries.Google.drive` module is designed to simplified the API access for some basic tasks. It does not rely on the Official Python API from Google. Instead, it is built on the [HTTP REST API](https://developers.google.com/drive/api/v3/reference).

See also:
* [Introduction to Google Drive API](https://developers.google.com/drive/api/v3/about-sdk)
* [Introduction to the Google Sheets API](https://developers.google.com/sheets/api/guides/concepts)
* [Google Drive API Reference](https://developers.google.com/drive/api/v3/reference)
* [Google Sheet API Reference](https://developers.google.com/sheets/api/reference/rest)

## File ID
Each file in Google Drive, including Google Docs, Google Sheets and Google Slides, is identified by a file ID. File ID stays the same for the life of the file, even if the file is renamed. For Google Docs/Sheets/Slides, the file ID can be obtained from the URL. For example, in following the URL for a Google sheet:
```
https://docs.google.com/spreadsheets/d/1ZTzD1VaLKffRIw8JDhZyLJeUSIwbd7AsJtv8u5XyqrM/
```
The file ID is `1ZTzD1VaLKffRIw8JDhZyLJeUSIwbd7AsJtv8u5XyqrM`.

## Authentication
Google Drive API can be accessed by either an OAuth 2.0 access token or a API key. The credentials can be created at [Google API Console](https://console.cloud.google.com/apis/credentials). Also, you must [enable Google Drive API](https://developers.google.com/drive/api/v2/enable-drive-api) or Google Sheet API in your account.

### API Key
API key is intended to be used for accessing data in your own account or accessing public data on your behave.
To use an API key see: [Setting up API keys](https://support.google.com/googleapi/answer/6158862?hl=en&ref_topic=7013279)

### OAuth Access Token
OAuth token access is mainly for applications to access data of different users. Users need to grant the access through a OAuth consent screen in order for your application to access the data. Your own Google account is also considered as a user. However, for testing purpose or personal use, we can use the same Google account to enable the API, create credentials and grant access.

See also: [Using OAuth 2.0 to Access Google APIs](https://developers.google.com/identity/protocols/oauth2)

## Accessing Google Drive
The `Aries.Google.drive.GoogleDriveFile` class is the base class for accessing the API.

Example:
```
from Aries.Google.drive import GoogleDriveFile

# Initialize the access
drive_file = GoogleDriveFile(FILE_ID, api_key=API_KEY)

# Get the metadata
metadata = drive_file.metadata

# Get the revisions of the the file
revisions = drive_file.revisions
```

## Accessing Google Sheet
The `Aries.Google.drive.GoogleSheet` class extends the `Aries.Google.drive.GoogleDriveFile` class and provide additional methods for accessing data from a Google sheet.

Example:
```
from Aries.Google.drive import GoogleSheet

# Initialize the access with API key
google_sheet = GoogleSheet(FILE_ID, api_key=API_KEY)

# A Google sheet may contain multiple sheets
# Get all data from the first sheet in the Google sheet as a 2-D list
data = google_sheet.get_data_grid()

# Get the values of row number 2 from a sheet named "SheetA" as a list
row = google_sheet.get_row_data(2, sheet_name="SheetA")

# Get the values of column "C" from a sheet named "SheetA" as a list
row = google_sheet.get_col_data("C", sheet_name="SheetA")
```

To get the values of a data range, for example "A1:C2" of the first sheet:
```
response = google_sheet.value("A1:C2")
values = response.get("values")
```
The `values()` method does not return the data directly. Instead, it is returning a dictionary containing a key named `values` to hold the data, as well as a key named `range` holding the actual range of the data. The reason for this is that the response may not contain exactly the requested range, the users need to check if the `range` is exactly the range requested.

### Extending the GoogleSheet class
The methods in this class provide a limited access to the Google API. User can extent the class by creating their own subclass or functions. Most HTTP REST API can be accessed by calling `api.get_json()` or `api.post_json()` methods. These methods will take care of the authentication.

For example, if you would like to access [Method: spreadsheets.values.get](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get):
```
# Initialize the access with API key
google_sheet = GoogleSheet(FILE_ID, api_key=API_KEY)

# Parameter
data_range = "A1:C2"

# Get a JSON response from Google API
# The values API has the following endpoint:
# GET https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values/{range}
response = google_sheet.api.get_json(
    "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s" % (
        google_sheet.file_id,
        data_range
    )
)
```
