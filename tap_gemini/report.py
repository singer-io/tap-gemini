# coding=utf-8
"""
Yahoo Gemini API reporting
"""

import csv
import datetime
import logging
import time

LOGGER = logging.getLogger(__name__)

YAHOO_REPORT_ENDPOINT = 'reports/custom'
DATE_FORMAT = '%Y-%m-%d'


class GeminiReport:
    """
    Yahoo Gemini Native & Search API Report

    https://developer.yahoo.com/nativeandsearch/guide/reporting/
    """

    def __init__(self, session, report_definition: dict, poll_interval: float = 1.):
        self.report_definition = report_definition
        self.session = session
        self.poll_interval = poll_interval
        self.job_id = None
        self.download_url = None

    @staticmethod
    def build_definition(advertiser_ids: list, cube: str, field_names: list,
                         start_date: datetime.date, end_date: datetime.date = None,
                         filters: list = None):
        """
        Build report definition

        https://developer.yahoo.com/nativeandsearch/guide/reporting/
        """

        if end_date is None:
            end_date = datetime.date.today()

        # Use ISO formatting
        start_date = start_date.strftime(DATE_FORMAT)
        end_date = end_date.strftime(DATE_FORMAT)

        # Mandatory filters
        _filters = [
            # Accounts
            {'field': 'Advertiser ID', 'operator': 'IN', 'values': advertiser_ids},
            # Time range
            {'field': 'Day', 'from': start_date, 'operator': 'between', 'to': end_date}
        ]
        if filters:
            # Optional filters
            _filters.extend(filters)

        # Build report definition
        return dict(
            cube=cube,
            fields=[dict(field=str(field_name)) for field_name in field_names],
            filters=_filters
        )

    def submit(self) -> str:
        """
        Submit a report request and retrieve a job ID number for polling

        :returns: Job ID
        """

        data = self.session.call(
            method='POST',
            endpoint=YAHOO_REPORT_ENDPOINT,
            json=self.report_definition
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

        endpoint = "{}/{}".format(YAHOO_REPORT_ENDPOINT, job_id)

        # Repeatedly poll the reporting server until the data is ready to download
        n_attempts = 0
        status = 'submitted'

        while True:
            n_attempts += 1

            # Time delay (minimum one second)
            secs = (max(1.0, self.poll_interval) + 0.1) ** n_attempts

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

        # Yield data rows from the CSV stream
        yield from csv.DictReader(data, fieldnames=headers)

    def parse_timestamps(self, timestamp: datetime.datetime) -> datetime.datetime:
        """Parse timestamps and insert time zone info"""
        raise NotImplementedError()

    @property
    def advertiser_id(self) -> int:
        """
        First advertiser ID

        When multiple account IDs are involved, the first advertiser ID should be used as the
        advertiserID parameter for the subsequent GET reporting call. For example, for a JSON
        sample request, with “123456” as the first advertiser ID, make a GET call to
        /reports/custom/{JobId}?advertiserId=123456
        """
        # Iterate over filters in report request
        for report_filter in self.report_definition['filters']:
            if report_filter['field'] == 'Advertiser ID':
                try:
                    return report_filter['value']
                except KeyError:
                    return report_filter['values'][0]

        raise ValueError('No advertiser ID specified')

    @property
    def end_date(self) -> datetime.date:
        """The end of the time range for this report"""
        for report_filter in self.report_definition['filters']:
            if report_filter['field'] == 'Day':
                return report_filter['to']
        raise ValueError('No date range specified')

    @property
    def tags(self) -> dict:
        """Tags to provide meta-data to metric messages"""
        return dict(
            endpoint=YAHOO_REPORT_ENDPOINT,
            cube=self.report_definition['cube']
        )
