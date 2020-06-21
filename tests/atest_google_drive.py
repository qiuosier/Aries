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

    google_client_id = None
    google_client_secret = None
    google_refresh_token = None
    google_access_token = None
    google_sheet_id = "1ZTzD1VaLKffRIw8JDhZyLJeUSIwbd7AsJtv8u5XyqrM"

    @classmethod
    def setUpClass(cls):
        cls.google_client_id = os.environ.get("GOOGLE_CLIENT_ID")
        cls.google_client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
        cls.google_refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")

    def setUp(self):
        if not self.google_client_id:
            self.skipTest("GOOGLE_CLIENT_ID not found.")
        if not self.google_client_secret:
            self.skipTest("GOOGLE_CLIENT_SECRET not found.")
        if not self.google_refresh_token:
            self.skipTest("GOOGLE_REFRESH_TOKEN not found.")

    def get_access_token(self):
        if not self.google_access_token:
            self.google_access_token = GoogleOAuth(
                self.google_client_id,
                self.google_client_secret
            ).refresh_access_token(self.google_refresh_token)
            self.assertIsNotNone(self.google_access_token)
        return self.google_access_token

    def test_access_google_sheet(self):
        google_sheet = GoogleSheet(
            file_id=self.google_sheet_id,
            access_token=self.get_access_token()
        )
        data = google_sheet.get_data_grid()
        self.assertGreater(len(data), 1)
        self.assertEqual(data[0][0], "A1")

        row = google_sheet.get_row_data(2, sheet_name="Sheet1")
        self.assertEqual(row[2], "C3")
        self.assertEqual(len(row), 3)
