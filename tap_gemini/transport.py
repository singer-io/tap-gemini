# coding=utf-8
"""
Yahoo Gemini API transport layer via a HTTP session
"""

import configparser
import logging
import argparse
import datetime
import getpass
import http
import urllib.parse

import requests

LOGGER = logging.getLogger(__name__)

CONFIG_FILE = 'yahoo.ini'


class GeminiSession(requests.Session):
    """Yahoo Gemini HTTP API Session"""

    def __init__(self, client_id: str, access_token: str = None, user_agent: str = None,
                 sandbox: bool = False):

        config = load_config()

        self.config = config

        # Initialise HTTP session
        super().__init__()

        self._access_token = access_token

        self.api_version = self.config['API']['api_version']

        self.client_id = client_id

        # Build API base URL
        if sandbox:
            base_url_format = self.config['API']['base_url_format']
        else:
            base_url_format = self.config['API']['sandbox_url_format']

        self.base_url = base_url_format.format(self.api_version)

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
            auth_data = self.authenticate()
            access_token = auth_data['access_token']
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
        token = getpass.getpass('Refresh token:')

        if not token:
            self.request_authentication()
            raise NotImplementedError()

        return token

    def _request_authentication(self) -> requests.Response:
        """
        Authorization Code Flow for Server-side Apps

        Step 2: Get an authorization URL and authorize access
        https://developer.yahoo.com/oauth2/guide/flows_authcode/

        :returns: Redirected HTTP response
        """

        return self.post(
            url=self.config['API']['authorization_url'],
            data=dict(
                client_id=self.client_id,
                redirect_uri='oob',
                response_type='code'
            )
        )

    def request_authentication(self) -> str:
        """
        Authorization Code Flow for Server-side Apps

        Step 3: User redirected for access authorization
        https://developer.yahoo.com/oauth2/guide/flows_authcode
        A successful response to request_auth initiates a 302 redirect to Yahoo where the user can
        authorize access.

        :return: Authorization URL
        """
        response = self._request_authentication()

        # Show redirection history
        for redirect_response in response.history:
            LOGGER.info(redirect_response.url)

        return response.url

    def authenticate(self) -> dict:
        """
        Exchange refresh token for new access token

        Authentication via authorization code grant. Explicit grant flow step 5:
        https://developer.yahoo.com/oauth2/guide/flows_authcode/

        You must follow the first few steps (to step 4) to generate a refresh token.
        Attempt to connect using existing access token or refresh it if it's expired.

        :returns: Authentication meta-data
        """
        post_params = dict(
            url=self.config['API']['authentication_url'],
            data=dict(
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
                grant_type='refresh_token',
                # No redirect URL (server-side authentication)
                redirect_uri='oob',
            ),
            auth=False
        )
        print(post_params)
        response = self.post(**post_params)
        data = response.json()

        # Parse seconds
        data['expires_in'] = datetime.timedelta(seconds=int(data['expires_in']))

        return data

    def build_url(self, endpoint: str) -> str:
        """Build the URI for the specified endpoint"""
        return urllib.parse.urljoin(self.base_url, endpoint)

    def request(self, *args, **kwargs) -> requests.Response:
        """Wrapper for requests methods, implement error handling"""

        # Make HTTP request
        response = super().request(*args, **kwargs)

        # Log HTTP headers
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


def debug_url(url: str):
    """Retrieve a URL via HTTP"""
    response = requests.get(url=url)
    try:
        response.raise_for_status()
    except requests.HTTPError as http_error:
        LOGGER.error(http_error)
        LOGGER.error(http_error.response.text)
        for arg in http_error.args:
            LOGGER.error(arg)
        raise
    print(response.text)


def debug():
    """Test API connection"""

    # Authenticate
    session = GeminiSession(
        client_id=input('Client ID:'),
        access_token=getpass.getpass('Access token (leave blank to authenticate):')
    )

    data = session.call(endpoint='')

    print(data)


def load_config() -> configparser.ConfigParser:
    """Load API configuration file"""

    config = configparser.ConfigParser()

    config.read(CONFIG_FILE)

    return config


def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help="Debug API connection")

    args = parser.parse_args()

    if args.debug:
        debug()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
