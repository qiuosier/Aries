import requests
import logging
logger = logging.getLogger(__name__)


class GoogleOAuth:
    token_endpoint = "https://www.googleapis.com/oauth2/v4/token"
    auth_endpoint = "https://accounts.google.com/o/oauth2/v2/auth"

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def authentication_url(self, scope, redirect_uri):
        auth_url = "%s?scope=%s&" \
                   "access_type=offline&" \
                   "include_granded_scopes=true&" \
                   "redirect_uri=%s&" \
                   "client_id=%s&" \
                   "response_type=code" % \
                   (
                       self.auth_endpoint,
                       scope,
                       redirect_uri,
                       self.client_id
                   )
        return auth_url

    def exchange_token(self, auth_code, redirect_uri):
        response = requests.post(GoogleOAuth.token_endpoint, {
            "code": auth_code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code"
        }).json()
        return response

    def refresh_access_token(self, refresh_token):
        response = requests.post(GoogleOAuth.token_endpoint, {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token"
        }).json()
        return response.get("access_token")
