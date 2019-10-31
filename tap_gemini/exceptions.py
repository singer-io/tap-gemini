"""
Gemini API errors
"""

import requests


class GeminiHTTPError(requests.HTTPError):
    """
    Yahoo Gemini API Error Codes and Responses

    https://developer.verizonmedia.com/nativeandsearch/guide/v1-api/error-responses.html
    """
    pass


class InternalServerError(GeminiHTTPError):
    pass


class UnsupportedFeatureError(GeminiHTTPError):
    pass


class InvalidInputError(GeminiHTTPError):
    pass


class AuthorizationError(GeminiHTTPError):
    pass


class ServiceUnavailableError(GeminiHTTPError):
    pass


class RequestTimeoutError(GeminiHTTPError):
    pass


class AccountInSyncReadOnlyError(GeminiHTTPError):
    pass


class TooManyRequestsError(GeminiHTTPError):
    pass


class UnknownReportingError(GeminiHTTPError):
    pass


class RequestsConflictError(GeminiHTTPError):
    pass


class NotFoundError(GeminiHTTPError):
    pass
