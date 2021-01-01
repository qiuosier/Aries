import requests
import logging
logger = logging.getLogger(__name__)


class OAuth2:
    def __init__(self, client_id, client_secret, auth_endpoint, token_endpoint) -> None:
        super().__init__()
        self.auth_endpoint = auth_endpoint
        self.token_endpoint = token_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = None

    def authentication_url(self, scope, redirect_uri="urn:ietf:wg:oauth:2.0:oob", response_type="code", **kwargs):
        """Generates the authentication URL for user to authenticate

        Args:
            scope (str or list): Scopes/Permissions to be requested.
            redirect_uri: The redirect URI after authentication.
                The default redirect_uri is a special value,
                which means to display the authentication code in the browser.

        Returns: A URL for user to authenticate.

        """
        self.scope = scope
        if isinstance(scope, list):
            scope = "%20".join(scope)

        auth_url = "%s?scope=%s&" \
                   "redirect_uri=%s&" \
                   "client_id=%s&" \
                   "response_type=%s" % \
                   (
                       self.auth_endpoint,
                       scope,
                       redirect_uri,
                       self.client_id,
                       response_type
                   )
        for key, val in kwargs.items():
            auth_url += "&%s=%s" % (key, val)
        return auth_url

    def exchange_token(self, auth_code, redirect_uri="urn:ietf:wg:oauth:2.0:oob", scope=None):
        data = {
            "code": auth_code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code"
        }

        if scope:
            data["scope"] = scope
        elif self.scope:
            data["scope"] = self.scope

        response = requests.post(self.token_endpoint, data).json()
        return response

    def refresh_access_token(self, refresh_token, scope=None):
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token"
        }

        if scope:
            data["scope"] = scope
        elif self.scope:
            data["scope"] = self.scope

        response = requests.post(self.token_endpoint, data).json()
        if response.get("error"):
            logger.error("Error when getting access token: %s" % response.get("error"))
        return response.get("access_token")


class GoogleOAuth:
    """
    See Also:
        Using OAuth 2.0 to Access Google APIs
        https://developers.google.com/identity/protocols/oauth2

    """

    token_endpoint = "https://www.googleapis.com/oauth2/v4/token"
    auth_endpoint = "https://accounts.google.com/o/oauth2/v2/auth"

    def __init__(self, client_id, client_secret):
        """
        OAuth 2.0 client IDs can be obtained at Google API Console:
        https://console.developers.google.com/

        Args:
            client_id: Google OAuth client ID
            client_secret: Google OAuth client secret
        """
        self.client_id = client_id
        self.client_secret = client_secret

    def authentication_url(self, scope, redirect_uri="urn:ietf:wg:oauth:2.0:oob"):
        """Generates the authentication URL for user to authenticate with Google

        Args:
            scope (str or list): OAuth 2.0 Scopes for Google APIs.
            redirect_uri: The redirect URI for Google to redirect after authentication.
                The default redirect_uri is a special value,
                which will cause Google to display the authentication code in the browser.

        Returns: A URL for user to authenticate with Google.

        See Also:
            OAuth 2.0 Scopes for Google APIs
            https://developers.google.com/identity/protocols/oauth2/scopes

        """
        if isinstance(scope, list):
            scope = "&".join(["scope=%s" % s for s in scope])
        else:
            scope = "scope=%s" % scope

        auth_url = "%s?%s&" \
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

    def exchange_token(self, auth_code, redirect_uri="urn:ietf:wg:oauth:2.0:oob"):
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
        if response.get("error"):
            logger.error("Error when getting access token: %s" % response.get("error"))
        return response.get("access_token")
