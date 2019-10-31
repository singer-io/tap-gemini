# coding=utf-8
"""
Yahoo Gemini API transport layer via a HTTP session

https://developer.yahoo.com/nativeandsearch/guide/
"""

import datetime
import http
import logging
import urllib.parse

import requests
import singer

import tap_gemini.exceptions

LOGGER = logging.getLogger(__name__)

BASE_URL_FORMAT = "https://api.gemini.yahoo.com/v{}/rest/"
SANDBOX_URL_FORMAT = "https://sandbox-api.gemini.yahoo.com/v{}/rest/"
AUTHORIZATION_URL = "https://api.login.yahoo.com/oauth2/request_auth"
AUTHENTICATION_URL = "https://api.login.yahoo.com/oauth2/get_token"

# https://developer.yahoo.com/nativeandsearch/guide/navigate-the-api/versioning/
DEFAULT_API_VERSION = 3

# Map HTTP error codes to specific exceptions
ERROR_MAP = {
    500: {
        'E10000_INTERNAL_SERVER_ERROR': tap_gemini.exceptions.InternalServerError,
        'E60000_UNKNOWN_REPORTING_ERROR': tap_gemini.exceptions.UnknownReportingError,
    },
    406: {
        'E30001_UNSUPPORTED_FEATURE': tap_gemini.exceptions.UnsupportedFeatureError
    },
    400: {
        'E40000_INVALID_INPUT': tap_gemini.exceptions.InvalidInputError
    },
    401: {
        'E50000_AUTHORIZATION_ERROR': tap_gemini.exceptions.AuthorizationError
    },
    503: {
        'E50003_SERVICE_UNAVAILABLE': tap_gemini.exceptions.ServiceUnavailableError
    },
    408: {
        'E40001_REQUEST_TIMEOUT': tap_gemini.exceptions.RequestTimeoutError
    },
    405: {
        'E40002_ACCOUNT_IN_SYNC_READ_ONLY': tap_gemini.exceptions.AccountInSyncReadOnlyError
    },
    403: {
        'E40003_TOO_MANY_REQUESTS': tap_gemini.exceptions.TooManyRequestsError
    },
    409: {
        'E40004_REQUEST_CONFLICT': tap_gemini.exceptions.RequestsConflictError
    },
    404: {
        'E40005_NOT_FOUND': tap_gemini.exceptions.NotFoundError
    },
}


class GeminiSession(requests.Session):
    """Yahoo Gemini HTTP API Session"""

    def __init__(self, client_id: str, client_secret: str, refresh_token: str,
                 access_token: str = None, user_agent: str = None, sandbox: bool = False,
                 api_version: int = None, session_options: dict = None):

        # Initialise HTTP session
        super().__init__()

        # Configure advanced HTTP options
        session_options = session_options or dict()
        for key, value in session_options.items():
            setattr(self, key, value)

        # Configure API access
        if api_version is None:
            api_version = DEFAULT_API_VERSION

        self.api_version = api_version

        # Build API base URL
        if sandbox:
            base_url_format = SANDBOX_URL_FORMAT
        else:
            base_url_format = BASE_URL_FORMAT
        self.base_url = base_url_format.format(self.api_version)

        # Credentials
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = access_token

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

    def _request_authentication(self) -> requests.Response:
        """
        Authorization Code Flow for Server-side Apps

        Step 2: Get an authorization URL and authorize access
        https://developer.yahoo.com/oauth2/guide/flows_authcode/

        :returns: Redirected HTTP response
        """

        return self.post(
            url=AUTHORIZATION_URL,
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
            LOGGER.info("REDIRECT: %s", redirect_response.url)

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
        response = self.post(
            url=AUTHENTICATION_URL,
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
        data = response.json()

        # Parse seconds
        data['expires_in'] = datetime.timedelta(seconds=int(data['expires_in']))

        return data

    def build_url(self, endpoint: str) -> str:
        """Build the URI for the specified endpoint"""
        return urllib.parse.urljoin(self.base_url, endpoint)

    @staticmethod
    def log_response_headers(response: requests.Response):
        """Log HTTP headers"""
        for header, value in response.request.headers.items():

            # Obfuscate sensitive info
            if header == 'Authorization':
                value = '************************'

            LOGGER.debug("REQUEST %s: %s", header, value)
        for header, value in response.headers.items():
            LOGGER.debug("RESPONSE %s: %s", header, value)

    def request(self, method: str, url: str, *args, **kwargs) -> requests.Response:
        """Wrapper for requests methods, implement error handling"""

        # Make HTTP request
        response = super().request(method, url, *args, **kwargs)
        self.log_response_headers(response)

        try:
            response.raise_for_status()

        # Handle HTTP errors
        except requests.HTTPError as http_error:

            # Clear authentication info
            if response.status_code == http.HTTPStatus.UNAUTHORIZED:
                del self.access_token

            # De-serialise JSON response
            data = response.json()

            # Get one or more errors returned from the API
            errors = list()

            # By default, we expect multiple errors, so extend the list
            try:
                errors.extend(data['errors'])
            # If a single error occurs then append it to the list
            except KeyError:
                errors.append(data['error'])

            # Log all errors
            for error in errors:
                LOGGER.error(error)

            # Raise an appropriate specific exception for the first error encountered
            for error in errors:
                gemini_error = ERROR_MAP[response.status_code][error['code']]

                raise gemini_error(error) from http_error

        return response

    def call(self, method: str = 'GET', endpoint: str = '', tags: dict = None, **kwargs):
        """
        Make a call to an API endpoint and return response data

        :rtype: May return a dictionary or a list
        :returns: Endpoint response
        """
        if tags is None:
            tags = dict()

        url = kwargs.get('url', self.build_url(endpoint=endpoint))

        # Singer HTTP response timer
        tags = dict(  # meta-data
            url=url,
            method=method,
            endpoint=endpoint,
            params=kwargs.get('params'),
            json=kwargs.get('json'),
            data=kwargs.get('data'),
            **tags
        )
        with singer.metrics.Timer(metric='http_request_timer', tags=tags):
            # Retrieve HTTP response
            response = self.request(method=method, url=url, **kwargs)

        data = response.json()

        api_response = data.pop('response')

        # Raise exceptions
        errors = data.pop('errors')

        if errors:
            for error in errors:
                LOGGER.error(error)
            raise RuntimeError(errors)

        return api_response

    @property
    def advertisers(self) -> list:
        """https://developer.yahoo.com/nativeandsearch/guide/advertiser.html"""
        return self.call(endpoint='advertiser', params=dict(mr=500))

    @property
    def dictionary(self) -> list:
        """https://developer.yahoo.com/nativeandsearch/guide/resources/data-dictionary/"""
        return self.call(endpoint='dictionary')
