# coding=utf-8
"""
Yahoo Gemini Native & Search API Objects

API endpoints: https://developer.yahoo.com/nativeandsearch/guide/api-endpoints/

Account structure objects: https://developer.yahoo.com/nativeandsearch/guide/objects.html
"""

import datetime

import pytz


class Object:
    """
    Object base class

    https://developer.yahoo.com/nativeandsearch/guide/objects.html

    The Native & Search API exposes lastUpdateDate and createdDate as read-only fields in API
    responses for all Native & Search API objects. These fields provide UNIX timestamps for when an
    object was created and last updated.
    """
    session = None
    edge = None

    def __init__(self, session=None, identifier: int = None, data: dict = None):
        """Either retrieve object data from the API or input it as a dictionary"""

        self.session = session

        self.id = identifier
        self.createdDate = None
        self.lastUpdateDate = None

        self.load(data=data)

    def load(self, data: dict = None):
        """Initialise attributes, possibly by retrieving data from API"""

        if data is None:
            data = self.get()

        # Save data to this object's attributes
        for key, value in data.items():
            setattr(self, key, value)

    @property
    def endpoint(self) -> str:
        return '{edge}/{id}'.format(edge=self.edge, id=self.id).casefold()

    def get(self) -> dict:
        """Retrieve an object's data by its unique ID number"""
        return self.session.call(method='GET', endpoint=self.endpoint)

    @classmethod
    def list(cls) -> list:
        """List all objects"""
        objects = list()

        for data in cls.list_data():
            # Instantiate object
            obj = cls(data=data)

            objects.append(obj)

        return objects

    @classmethod
    def list_data(cls, session=None) -> list:
        """List all objects of this type"""
        objects = list()

        if session is None:
            session = cls.session

        for obj in session.call(method='GET', endpoint=cls.edge):
            # Parse timestamps
            # The Native & Search API exposes lastUpdateDate and createdDate as read-only fields in
            # API responses for all Native & Search API objects. These fields provide UNIX
            # timestamps for when an object was created and last updated.
            for key in {'lastUpdateDate', 'createdDate'}:
                obj[key] = datetime.datetime.fromtimestamp(obj[key] / 1000, tz=pytz.UTC)

            objects.append(obj)

        return objects

    def to_dict(self) -> dict:
        """Build a dictionary containing this object's data"""

        data = dict()

        for key, value in vars(self).items():
            if key.startswith('_'):
                continue

            data[key] = value

        return data


class Advertiser(Object):
    """
    Advertiser

    https://developer.yahoo.com/nativeandsearch/guide/advertiser.html
    """

    edge = 'advertiser'

    def __init__(self, session=None, identifier=None, data=None):
        self.advertiserName = None
        self.timezone = None
        self.currency = None
        self.type = None
        self.status = None

        super().__init__(session, identifier, data)


class Campaign(Object):
    """
    Campaign

    https://developer.yahoo.com/nativeandsearch/guide/campaigns.html
    """

    edge = 'campaign'


class AdGroup(Object):
    """
    Ad Group

    https://developer.yahoo.com/nativeandsearch/guide/adgroup.html
    """
    edge = 'adgroup'


class Ad(Object):
    """
    Ad

    https://developer.yahoo.com/nativeandsearch/guide/ad.html
    """
    edge = 'ad'


class Keyword(Object):
    """
    Keyword

    https://developer.yahoo.com/nativeandsearch/guide/keyword.html
    """
    edge = 'keyword'


class TargetingAttribute(Object):
    """
    Targeting Attribute
    """
    edge = 'targetingattribute'


class AdExtensions(Object):
    """
    Ad Extensions
    """
    edge = 'adextension'


class SharedSitelink(Object):
    """
    Shared Sitelink
    """
    edge = 'sharedsitelink'


class SharedSitelinkSetting(Object):
    """
    Shared Sitelink Setting
    """
    edge = 'sharedsitelink'


class AdSiteSetting(Object):
    """
    Ad Site Setting
    """
    edge = 'adsitesetting'
