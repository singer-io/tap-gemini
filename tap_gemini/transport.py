# coding=utf-8
"""
Yahoo Gemini API transport layer via a HTTP session
"""

import logging
import datetime
import getpass
import http
import urllib.parse

import requests

LOGGER = logging.getLogger(__name__)

BASE_URL_FORMAT = "https://api.gemini.yahoo.com/v{}/rest/"


class GeminiSession(requests.Session):
    """Yahoo Gemini HTTP API Session"""

    def __init__(self, client_id: str, api_version: int = 3, access_token: str = None,
                 user_agent: str = None):

        # Initialise HTTP session
        super().__init__()

        self._access_token = access_token

        self.client_id = client_id
        self.base_url = BASE_URL_FORMAT.format(api_version)

        # Update HTTP headers
        self.headers.update(self.headers_extra)

        # Overwrite user agent
        if user_agent:
            self.headers.update({'User-Agent': user_agent})

    @property
    def headers_extra(self) -> dict:
        """Extra HTTP headers for this API"""
        return {
            'Authorization': 'Bearer {token}'.format(token=self.access_token)
        }

    @property
    def access_token(self) -> str:
        """OAuth 2.0 access token"""
        access_token = self._access_token

        # Get new token
        if not access_token:
            access_token = self.refresh_access_token()
            self.access_token = access_token

        return access_token

    @access_token.deleter
    def access_token(self) -> None:
        """OAuth 2.0 Access Token"""
        self._access_token = None

    @access_token.setter
    def access_token(self, new_token: str) -> None:
        """Access token setter"""
        self._access_token = new_token

    @property
    def client_secret(self) -> str:
        """OAuth Client secret"""
        return getpass.getpass('Client secret:')

    @property
    def refresh_token(self) -> str:
        """OAuth refresh token"""
        return getpass.getpass('Refresh token:')

    def refresh_access_token(self):
        """
        Exchange refresh token for new access token

        Authentication via authorization code grant
        Explicit grant flow: https://developer.yahoo.com/oauth2/guide/flows_authcode/

        I have already followed the first few steps (to step 4) to generate a refresh token.
        Attempt to connect using existing access token or refresh it if it's expired.

        https://developer.yahoo.com/oauth2/guide/flows_authcode/#step-5-exchange-refresh-token-for-new-access-token

        :returns: Authentication meta-data
        """
        response = self.post(
            url='https://api.login.yahoo.com/oauth2/get_token',
            data=dict(
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
                grant_type='refresh_token',
                redirect_uri='oob',  # no redirect URL (server-side authentication)
            ),
            auth=False
        )
        data = response.json()

        token = data.pop('access_token')
        data['expires_in'] = datetime.timedelta(seconds=int(data['expires_in']))

        # Debugging info
        for key, value in data.items():

            # Obfuscate
            if key == 'refresh_token':
                value = '***********************'

            LOGGER.debug("REFRESH %s: %s", key, value)

        return token

    def build_url(self, endpoint: str) -> str:
        """Build the URI for the specified endpoint"""
        return urllib.parse.urljoin(self.base_url, endpoint)

    def request(self, *args, **kwargs) -> requests.Response:
        """Wrapper for requests methods, implement error handling"""

        # Make HTTP request
        response = super().request(*args, **kwargs)

        # Log HTTP headers
        if LOGGER.getEffectiveLevel() <= logging.DEBUG:
            for header, value in response.request.headers.items():

                # Obfuscate sensitive info
                if header == 'Authorization':
                    value = '************************'

                LOGGER.debug("REQUEST %s: %s", header, value)
            for header, value in response.headers.items():
                LOGGER.debug("RESPONSE %s: %s", header, value)

        try:
            response.raise_for_status()

        # Handle HTTP errors
        except requests.HTTPError as http_error:
            response = http_error.response

            # Clear authentication info
            if response.status_code == http.HTTPStatus.UNAUTHORIZED:
                del self.access_token
                raise

            # Parse response
            data = response.json()

            # Log error messages
            for key, value in data.items():
                LOGGER.error("%s: %s", key, value)

            for error in data.get('errors', dict()):
                for key, value in error.items():
                    LOGGER.error("%s: %s", key, value)

            raise

        return response

    def call(self, endpoint: str, **kwargs) -> dict:
        """Make a call to an API endpoint and return response data"""

        url = kwargs.get('url', self.build_url(endpoint=endpoint))

        LOGGER.info(url)

        # Retrieve HTTP response
        response = self.get(url=url, **kwargs)

        data = response.json()

        api_response = data.pop('response')

        # Raise exceptions
        errors = data.pop('errors')

        if errors:
            for error in errors:
                LOGGER.error(error)
            raise RuntimeError(errors)

        return api_response
