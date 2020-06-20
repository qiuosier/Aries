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


class TestGoogleDrive(AriesTest):

    google_client_id = None
    google_client_secret = None
    google_refresh_token = None

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

    def test_get_access_token(self):
        access_token = GoogleOAuth(
            self.google_client_id,
            self.google_client_secret
        ).refresh_access_token(self.google_refresh_token)
        self.assertIsNotNone(access_token)
