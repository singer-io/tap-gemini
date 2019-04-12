# coding=utf-8
"""
Yahoo Gemini API reporting
"""

import csv
import datetime
import logging
import time

import singer

LOGGER = logging.getLogger(__name__)

YAHOO_REPORT_ENDPOINT = 'reports/custom'
DATE_FORMAT = '%Y-%m-%d'


def build_report_definition(config, state, stream, start_date: datetime.date,
                            end_date: datetime.date) -> dict:
    """
    Convert a JSON schema to a Gemini report request
    
    JSON schema:
    
        http://json-schema.org/
    
    Gemini: Working with cubes:
    
        https://developer.yahoo.com/nativeandsearch/guide/reporting/
    """

    # TODO implement state
    # LOGGER.debug("STATE: %s", state)

    field_names = stream.schema['properties'].keys()
    advertiser_ids = config['advertiser_ids']

    # Use ISO formatting
    start_date = start_date.strftime(DATE_FORMAT)
    end_date = end_date.strftime(DATE_FORMAT)

    # Build report definition
    return dict(
        cube=stream.stream,
        fields=[
            dict(field=field_name) for field_name in field_names
        ],
        filters=[
            # Mandatory filters

            # Accounts
            {'field': 'Advertiser ID', 'operator': 'IN', 'values': advertiser_ids},

            # Time range
            {'field': 'Day', 'from': start_date, 'operator': 'between', 'to': end_date}
        ]
    )


class GeminiReport:
    """Yahoo Gemini Report"""

    def __init__(self, session, report_definition: dict, poll_interval: float = 1.):
        self.report_definition = report_definition
        self.session = session
        self.poll_interval = poll_interval
        self.job_id = None
        self.download_url = None

    def submit(self) -> str:
        """
        Submit a report request and retrieve a job ID number for polling

        :returns: Job ID
        """

        data = self.session.call(
            method='post',
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

        if job_id is None:
            if self.job_id:
                job_id = self.job_id
            else:
                job_id = self.submit()

        endpoint = "{}/{}".format(YAHOO_REPORT_ENDPOINT, job_id)

        # Repeatedly poll the reporting server until the data is ready to download
        n_attempts = 0
        while True:
            n_attempts += 1
            LOGGER.info('JOB ID: %s POLL Attempt #%s', job_id, n_attempts)

            response = self.session.call(
                endpoint=endpoint,
                params={'advertiserId': self.advertiser_id}
            )

            # Check the report status
            status = response['status']

            # If the report is ready then a download URL is given:
            if status == 'completed':
                download_url = response['jobResponse']
                break

            # The job is in queue but work on it has yet to commence.
            elif status == 'submitted':
                time.sleep(self.poll_interval * 3)

            # Short time delay before polling again
            elif status == 'running':
                time.sleep(self.poll_interval)

            else:
                LOGGER.error('Unknown server response: %s', response)
                raise ValueError(response)

        self.download_url = download_url

        return download_url

    def _stream(self) -> iter:
        """"
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

    def stream(self) -> iter:
        """Wrapper for data streaming function to implement Singer metrics"""

        # Generate data and count rows
        with singer.metrics.Counter(metric='record_count', tags=self.tags) as counter:
            for row in self._stream():
                counter.increment()
                yield row

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

    def run(self) -> iter:
        """
        Retrieve data from the Yahoo Gemini Report API by submitting a report request, waiting for
        it to by ready, then streaming the report data.

        https://developer.yahoo.com/nativeandsearch/guide/reporting/
        """

        # Stream data rows
        with singer.metrics.Timer(metric='job_timer', tags=self.tags):
            yield from self.stream()

        # Save state on success
        singer.write_state(value=self.end_date)

    @property
    def tags(self) -> dict:
        """Tags to provide meta-data to metric messages"""
        return dict(
            endpoint=YAHOO_REPORT_ENDPOINT,
            cube=self.report_definition['cube']
        )
