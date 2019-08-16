import requests
import logging
logger = logging.getLogger(__name__)


class GoogleOAuth:
    token_endpoint = "https://www.googleapis.com/oauth2/v4/token"

    @staticmethod
    def refresh_access_token(client_id, client_secret, refresh_token):
        response = requests.post(GoogleOAuth.token_endpoint, {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token"
        }).json()
        return response.get("access_token")
