# coding=utf-8
"""
Yahoo Gemini API reporting
"""

import logging
import datetime
import time
import csv

import singer

LOGGER = logging.getLevelName(__name__)

YAHOO_REPORT_ENDPOINT = 'reports/custom'


def build_date(timestamp: datetime.datetime) -> datetime.date:
    """Create a native Python date object using duck-typing"""
    return datetime.date(
        year=timestamp.year,
        month=timestamp.month,
        day=timestamp.day,
    )


def build_report_definition(config: dict, state: dict, stream: dict, start_date: datetime.date,
                            end_date: datetime.date) -> dict:
    """
    Convert a JSON schema to a Gemini report request

    JSON schema:

        http://json-schema.org/

    Gemini: Working with cubes:

        https://developer.yahoo.com/nativeandsearch/guide/reporting/
    """

    LOGGER.debug(state)

    field_names = stream['schema']['properties'].keys()
    advertiser_ids = config['advertiser_ids']

    # Ensure we're using dates rather than date-times
    start_date, end_date = map(build_date, (start_date, end_date))

    return dict(
        cube=stream['stream'],
        fields=[
            dict(field=field_name) for field_name in field_names
        ],
        filters=[
            # Mandatory filters

            # Time range
            {
                'field': 'Day',
                'from': start_date.isoformat(),
                'operator': 'between',
                'to': end_date.isoformat()
            },

            # Account IDS
            {
                'field': 'Advertiser ID',
                'operator': 'IN',
                'values': advertiser_ids
            },
        ]
    )


class GeminiReport:
    """Yahoo Gemini Report"""

    def __init__(self, session, report_definition: dict, poll_interval: float = 0.5):
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
            endpoint=YAHOO_REPORT_ENDPOINT,
            json=self.report_definition
        )

        # Raise errors
        if data['status'] != 'submitted':
            raise RuntimeError('Report submission failure', data)

        job_id = data['jobId']

        self.job_id = job_id

        return job_id

    def poll(self) -> str:
        """
        Poll reporting server for a report download URL

        Get job status via a GET call to /reports/custom/{JobId}?advertiserId={advertiserId}

        :returns: URL of the report data to download
        """

        if not self.job_id:
            self.submit()

        endpoint = "{}/{}".format(YAHOO_REPORT_ENDPOINT, self.job_id)

        # Repeatedly poll the reporting server until the data is ready to download
        while True:

            response = self.session.call(endpoint=endpoint,
                                         params={'advertiserId': self.advertiser_id})

            # Check if the report is ready
            if response['status'] == 'completed':
                # If the report is ready then a download URL is given:
                download_url = response['jobResponse']
                break
            elif response['status'] == 'submitted':
                # The job is in queue but work on it has yet to commence.
                time.sleep(self.poll_interval * 3)
            elif response['status'] == 'running':
                # Short time delay before polling again
                time.sleep(self.poll_interval)
            else:
                LOGGER.error('Unknown server response: %s', response)
                raise ValueError(response)

        LOGGER.debug(download_url)

        self.download_url = download_url

        return download_url

    def stream(self) -> iter:
        """"
        Stream data rows from a CSV file, yielding an iterator with one dictionary per data row.
        """

        start_time = time.time()

        if not self.download_url:
            self.poll()

        # Stream report data CSV, line by line
        response = self.session.get(self.download_url, stream=True)
        data = response.iter_lines(decode_unicode=True)

        # Parse CSV format

        # Get headers by parsing first row
        with csv.reader(data) as reader:
            headers = next(reader)

        for string in headers:
            LOGGER.debug("HEADER: %s", string)

        # Yield rows (and count the total number of rows)
        n_rows = 0
        for row in csv.DictReader(data, fieldnames=headers):
            n_rows += 1
            yield row

        # Write metric messages
        message = dict(
            type='counter',
            metric='record_count',
            value=n_rows,
            tags=self.tags
        )
        singer.write_message(message=message)

        message = dict(
            type='timer',
            metric='report_download_duration',
            value=time.time() - start_time,
            tags=self.tags
        )
        singer.write_message(message=message)

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

        start_time = time.time()

        # Stream data rows
        yield from self.stream()

        # Metric message for entire report duration, including submission and polling
        message = dict(
            type='timer',
            metric='report_duration',
            value=time.time() - start_time,
            tags=self.tags
        )
        singer.write_message(message=message)

        # Save state on success
        singer.write_state(value=dict(end_date=self.end_date))

    @property
    def tags(self) -> dict:
        """Tags to provide meta-data to metric messages"""
        return dict(
            endpoint=YAHOO_REPORT_ENDPOINT,
            cube=self.report_definition['cube']
        )
