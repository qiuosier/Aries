import logging
import os
import sys
logger = logging.getLogger(__name__)
try:
    from ..test import AriesTest
    from ..oauth import GoogleOAuth
    from ..Google.drive import GoogleSheet
except:
    aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
    if aries_parent not in sys.path:
        sys.path.append(aries_parent)
    from Aries.test import AriesTest
    from Aries.oauth import GoogleOAuth
    from Aries.Google.drive import GoogleSheet


class TestGoogleSheet(AriesTest):
    """Contains tests for GoogleSheet class
    """
    google_client_id = None
    google_client_secret = None
    google_refresh_token = None

    google_access_token = None
    google_sheet_id = "1ZTzD1VaLKffRIw8JDhZyLJeUSIwbd7AsJtv8u5XyqrM"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Saves the credentials to class variables
        cls.google_client_id = os.environ.get("GOOGLE_CLIENT_ID")
        cls.google_client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
        cls.google_refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")

    def assert_sheet_data(self, google_sheet):
        """Checks GoogleSheet object
        """
        data = google_sheet.get_data_grid()
        self.assertGreater(len(data), 1)
        self.assertEqual(data[0][0], "A1")

        row = google_sheet.get_row_data(2, sheet_name="Sheet1")
        self.assertEqual(row[2], "C2")
        self.assertEqual(len(row), 3)

        col = google_sheet.get_column_data('C', sheet_name="Sheet1")
        self.assertEqual(col[0], "")
        self.assertEqual(col[1], "C2")
        self.assertGreaterEqual(len(col), 2)

        values = google_sheet.values("A1:C2").get("values")
        self.assertEqual(len(values), 2)
        self.assertEqual(values[0][0], "A1")

    def get_access_token(self):
        """Gets the Google access token.
        Tests will be skipped if any one of the credentials is not set in the environment variable.
        """
        if not self.google_access_token:
            # Skip the test if the credentials are not found.
            if not self.google_client_id:
                self.skipTest("GOOGLE_CLIENT_ID not found.")
            if not self.google_client_secret:
                self.skipTest("GOOGLE_CLIENT_SECRET not found.")
            if not self.google_refresh_token:
                self.skipTest("GOOGLE_REFRESH_TOKEN not found.")

            # Get a new access token
            self.google_access_token = GoogleOAuth(
                self.google_client_id,
                self.google_client_secret
            ).refresh_access_token(self.google_refresh_token)
            self.assertIsNotNone(self.google_access_token)
        return self.google_access_token

    def get_api_key(self):
        """Gets the Google API Key.
        Tests will be skipped if GOOGLE_API_KEY is not set in the environment variable.
        """
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            self.skipTest("GOOGLE_API_KEY not found.")
        return api_key

    def test_access_google_sheet_with_access_token(self):
        """Tests access Google sheet with access token
        """
        google_sheet = GoogleSheet(
            file_id=self.google_sheet_id,
            access_token=self.get_access_token()
        )
        self.assert_sheet_data(google_sheet)

    def test_access_google_sheet_with_api_key(self):
        """Tests access Google sheet with API key
        """
        google_sheet = GoogleSheet(
            file_id=self.google_sheet_id,
            api_key=self.get_api_key()
        )
        self.assert_sheet_data(google_sheet)
