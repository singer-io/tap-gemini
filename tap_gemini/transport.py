# coding=utf-8
"""
Yahoo Gemini API transport layer via a HTTP session
"""

import os.path
import logging
import datetime
import getpass
import http
import urllib.parse

import requests

logger = logging.getLogger(__name__)


class GeminiSession(requests.Session):
    """Yahoo Gemini HTTP API Session"""

    def __init__(self, client_id: str):

        # Initialise HTTP session
        super().__init__()

        # Update HTTP headers
        self.headers.update(self.headers_extra)

    @property
    def headers_extra(self) -> dict:
        return {
            'Authorization': 'Bearer {}'.format(self.access_token)
        }

    @property
    def access_token_key(self) -> str:
        """The key for the variable key-value store used to store the access token"""
        return self.config['local']['access_token_key']

    @property
    def access_token(self) -> str:
        """OAuth 2.0 access token"""
        try:
            access_token = Variable.get(self.access_token_key, None)
        except KeyError:
            access_token = ''

        # Get new token
        if not access_token:
            access_token = self.refresh_access_token()
            self.access_token = access_token

        return access_token

    @access_token.deleter
    def access_token(self) -> None:
        Variable.set(self.access_token_key, '')

    @access_token.setter
    def access_token(self, new_token: str) -> None:
        Variable.set(self.access_token_key, new_token)

    @property
    def client_id(self) -> str:
        return self.config['api']['client_id']

    @property
    def local_directory(self) -> str:
        """The local filesystem directory to store serialised data"""
        return os.path.join(os.path.expanduser('~'), self.settings['local']['path'])

    @property
    def credentials_path(self) -> str:
        return os.path.join(self.local_directory, self.config['local']['auth_file'])

    @property
    def credentials(self) -> dict:
        try:
            with open(self.credentials_path) as file:
                credentials = yaml.safe_load(file)
        except FileExistsError:
            client_secret = getpass.getpass('Client secret: ')
            refresh_token = getpass.getpass('Refresh token: ')

            credentials = dict(

            )

        return credentials

    @property
    def client_secret(self) -> str:
        return self.credentials['client_secret']

    @property
    def refresh_token(self) -> str:
        return self.credentials['refresh_token']

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
        logging.info('Client ID: {}'.format(self.client_id))
        response = self.post(
            url='https://api.login.yahoo.com/oauth2/get_token',
            data=dict(
                client_id=self.client_id,

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
            if key == 'refresh_token':
                value = '***********************'
            logger.debug("REFRESH {}: {}".format(key, value))

        return token

    @property
    def base_url(self) -> str:
        return self.config['api']['base_url']

    def build_url(self, endpoint: str) -> str:
        return urllib.parse.urljoin(self.base_url, endpoint)

    def request(self, *args, **kwargs) -> requests.Response:
        """Wrapper for requests methods, implement error handling"""

        # Make HTTP request
        response = super().request(*args, **kwargs)

        # Log HTTP headers
        if logger.getEffectiveLevel() <= logging.DEBUG:
            for header, value in response.request.headers.items():
                if header == 'Authorization':
                    value = '************************'
                logger.debug("REQUEST {}: {}".format(header, value))
            for header, value in response.headers.items():
                logger.debug("RESPONSE {}: {}".format(header, value))

        try:
            response.raise_for_status()

        # Handle HTTP errors
        except requests.HTTPError as e:
            response = e.response

            # Clear authentication info
            if response.status_code == http.HTTPStatus.UNAUTHORIZED:
                del self.access_token
                raise

            # Parse response
            data = response.json()

            # Log error messages
            for key, value in data.items():
                logger.error("{}: {}".format(key, value))

            for error in data.get('errors', dict()):
                for key, value in error.items():
                    logger.error("{}: {}".format(key, value))

            raise

        return response

    def call(self, *args, **kwargs):
        """Call API endpoint"""
        response = self.get(*args, **kwargs)
        data = response.json()

        response = data.pop('response')
        errors = data.pop('errors')
        if errors:
            for error in errors:
                logger.error(error)
            raise RuntimeError(errors)

        for key, value in data.items():
            logger.info("CALL {}: {}".format(key, value))

        return response
