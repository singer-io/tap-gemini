# coding=utf-8
"""
Yahoo Gemini API reporting

https://developer.yahoo.com/nativeandsearch/guide/reporting/
"""

import csv
import datetime
import logging
import time

import pytz

import tap_gemini.exceptions

LOGGER = logging.getLogger(__name__)

REPORT_ENDPOINT = 'reports'
CUSTOM_REPORT_ENDPOINT = REPORT_ENDPOINT + '/custom'
DEFAULT_POLL_INTERVAL = 1.0


class BooksClosedNotImplementedError(NotImplementedError):
    """Books Closed is not supported for this cube."""
    pass


class GeminiReport:
    """
    Yahoo Gemini Native & Search API Report

    https://developer.yahoo.com/nativeandsearch/guide/reporting/
    """

    def __init__(self, session, advertiser_ids: list, cube: str, field_names: list,
                 start_date: datetime.date, end_date: datetime.date = None, filters: list = None,
                 poll_interval: float = None):
        self.advertiser_ids = advertiser_ids
        self.cube = cube
        self.field_names = field_names
        self.start_date = start_date
        self.end_date = end_date or datetime.date.today()
        self.filters = filters or list()
        self.session = session
        self.poll_interval = float(poll_interval or DEFAULT_POLL_INTERVAL)
        self.job_id = None
        self.download_url = None

    @property
    def definition(self) -> dict:
        """
        Build report definition

        https://developer.yahoo.com/nativeandsearch/guide/reporting/
        """

        # Mandatory filters
        _filters = [

            # Accounts
            {
                'field': 'Advertiser ID',
                'operator': 'IN',
                'values': self.advertiser_ids
            },

            # Time range
            {
                'field': 'Day',
                'from': self.start_date.isoformat(), 'operator': 'between',
                'to': self.end_date.isoformat()
            }
        ]

        # Optional filters
        _filters.extend(self.filters)

        return dict(
            cube=self.cube,
            fields=[dict(field=str(field_name)) for field_name in self.field_names],
            filters=_filters
        )

    def submit(self) -> str:
        """
        Submit a report request and retrieve a job ID number for polling

        :returns: Job ID
        """

        data = self.session.call(
            method='POST',
            endpoint=CUSTOM_REPORT_ENDPOINT,
            json=self.definition
        )

        # Raise errors
        if data['status'] != 'submitted':
            raise RuntimeError('Report submission failure', data)

        job_id = data['jobId']

        self.job_id = job_id

        return job_id

    def poll(self, job_id: str = None) -> str:
        """
        Poll reporting server for a report download URL

        Get job status via a GET call to /reports/custom/{JobId}?advertiserId={advertiserId}

        :returns: URL of the report data to download
        """

        start_time = time.time()

        if job_id is None:
            if self.job_id:
                job_id = self.job_id
            else:
                job_id = self.submit()

        endpoint = "{}/{}".format(CUSTOM_REPORT_ENDPOINT, job_id)

        # Repeatedly poll the reporting server until the data is ready to download
        n_attempts = 0
        status = 'submitted'

        while True:
            n_attempts += 1

            # Time delay (minimum one second) with exponential back-off
            secs = (max(1.0, self.poll_interval) + 0.2) ** n_attempts

            response = self.session.call(
                endpoint=endpoint,
                params={'advertiserId': self.advertiser_id},
                tags=dict(
                    poll_attempt=n_attempts,
                    poll_time_seconds=time.time() - start_time,
                    poll_latest_status=status
                )
            )

            # Check the report status
            status = response['status']

            # If the report is ready then a download URL is given:
            if status == 'completed':
                download_url = response['jobResponse']
                break

            # The job is running or in a queue waiting to commence
            elif status in {'running', 'submitted'}:
                # Short time delay before polling again, exponential decay
                time.sleep(secs)

            else:
                LOGGER.error('Unknown poll status: %s', response)
                raise ValueError(response)

        self.download_url = download_url

        return download_url

    def stream(self) -> iter:
        """
        Retrieve data from the Yahoo Gemini Report API by submitting a report request,
        waiting for it to by ready, then streaming the report data.

        Stream data rows from a CSV file, yielding an iterator with one dictionary per data row.
        """

        if not self.download_url:
            self.poll()

        # Stream report data CSV, line by line
        response = self.session.get(self.download_url, stream=True)
        data = response.iter_lines(decode_unicode=True)

        # Parse CSV format

        # Get headers by parsing first row
        reader = csv.reader(data)
        headers = next(reader)  # list

        # Yield data rows (dictionaries) from the CSV stream
        yield from csv.DictReader(data, fieldnames=headers)

    @property
    def advertiser_id(self) -> int:
        """
        First advertiser ID

        When multiple account IDs are involved, the first advertiser ID should be used as the
        advertiserID parameter for the subsequent GET reporting call. For example, for a JSON
        sample request, with “123456” as the first advertiser ID, make a GET call to
        /reports/custom/{JobId}?advertiserId=123456
        """

        return self.advertiser_ids[0]

    @property
    def tags(self) -> dict:
        """Tags to provide meta-data to metric messages"""
        return dict(
            endpoint=REPORT_ENDPOINT,
            cube=self.cube
        )

    def close_of_business(self, date: datetime.date) -> dict:
        """
        Get the status of the books (are they closed for the day?)

        See: About Books Closed
        https://developer.yahoo.com/nativeandsearch/guide/reporting/
        """

        endpoint = REPORT_ENDPOINT + '/cob'
        date_string = date.strftime('%Y%m%d')

        def _close_of_business(cube: str = None) -> dict:
            """Implement this function"""

            # Mandatory parameters
            params = dict(
                advertiserId=self.advertiser_id,
                date=date_string,
            )

            # Optional parameter
            if cube is not None:
                params['cubeName'] = cube

            return self.session.call(
                endpoint=endpoint,
                params=params
            )

        try:
            return _close_of_business(cube=self.cube)

        # Handle unsupported reports
        except tap_gemini.exceptions.InvalidInputError:
            LOGGER.warning('Cube "%s" is not in the list of currently supported reports', self.cube)

            # Call the endpoint without specifying a cube
            LOGGER.info('Retrying close of business for "%s" with no cube specified', date)

            return _close_of_business()

    def are_books_closed(self, date: datetime.date) -> datetime.datetime:
        """
        Check whether books are closed for the specified date.

        :returns: Timezone-aware timestamp for the time when the books were closed.
        """

        status = self.close_of_business(date)

        books_closed = status['isDayClosed'] | status['isMonthClosed']

        # Completion percentage (what fraction of the books are closed)
        books_closed_ratio = status.get('dayProgressPercent')

        # The completion percentage is not provided if no cube is specified
        if books_closed_ratio is None:
            books_closed_ratio = 100 if books_closed else 0

        LOGGER.debug('CLOSE_OF_BUSINESS: %s %s (%s%%)', date, books_closed, books_closed_ratio)

        # Build timestamp
        return datetime.datetime.combine(
            date=date,
            time=datetime.time(0, tzinfo=pytz.timezone(status['advertiserTimezone']))
        )
