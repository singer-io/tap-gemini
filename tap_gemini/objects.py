# coding=utf-8
"""
Yahoo Gemini API objects and utility functions

https://developer.yahoo.com/nativeandsearch/guide/objects.html
"""


def list_advertisers(session) -> list:
    return session.call(endpoint='advertiser', params=dict(mr=500))
