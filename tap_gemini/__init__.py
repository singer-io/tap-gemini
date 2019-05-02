#!/usr/bin/env python3
# coding=utf-8
"""
Singer Tap for Yahoo Gemini

Singer:

    - All timestamps must use the RFC3339 standard.

Gemini API documentation:

    - https://developer.yahoo.com/nativeandsearch/guide/
    - https://developer.yahoo.com/nativeandsearch/guide/reporting/
"""

import datetime
import json
import os

import singer.metrics
import singer.utils
import singer.metadata
import singer.transform

import transport
import report
import api

__version__ = '0.0.2'

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "username",
    "password"
]

# Schema config
SCHEMAS_DIR = 'schemas'
METADATA_DIR = 'metadata'
KEY_PROPERTIES_DIR = 'key_properties'

# Map schema name to API object
OBJECT_MAP = dict(
    advertiser=api.Advertiser,
    campaign=api.Campaign,
    # TODO Implement further objects
    # adgroup=api.AdGroup
)

# Time windowing for running reports in chunks
# This prevents ERROR_CODE:10001 Max days window exceeded expected
MAX_WINDOW_DAYS = dict(
    search_stats=15,
    performance_stats=15,
    keyword_stats=400,
    product_ads=400,
    site_performance_stats=400,
)

# Maximum number of days to go back in time
# This prevents ERROR_CODE:10002 Max look back window exceeded expected
MAX_LOOK_BACK_DAYS = dict(
    performance_stats=15,
    keyword_stats=750,
    product_ads=400,
    site_performance_stats=400,
)


def get_abs_path(path: str) -> str:
    """Build the absolute path on the local filesystem"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_directory(dir_path: str) -> dict:
    """Load all configuration files in the specified directory"""

    abs_path = get_abs_path(dir_path)

    schemas = dict()

    # Iterate over schema files
    for filename in os.listdir(abs_path):
        path = os.path.join(abs_path, filename)
        name = filename.replace('.json', '').casefold().replace(' ', '_')

        # Parse JSON
        with open(path) as file:
            try:
                schemas[name] = json.load(file)
            except json.JSONDecodeError:
                singer.log_error('JSON syntax error in file "%s"', file.name)
                raise

            singer.log_debug('Loaded "%s"', file.name)

    return schemas


def load_schemas() -> dict:
    """Load schemas from config files"""

    schemas = load_directory(SCHEMAS_DIR)

    # Build singer.Schema objects from raw JSON data
    for name, data in schemas.items():
        schemas[name] = singer.Schema.from_dict(data=data)

    return schemas


def load_metadata() -> dict:
    """Load metadata from config files"""

    return load_directory(METADATA_DIR)


def load_key_properties() -> dict:
    """Load key properties from config files"""

    return load_directory(KEY_PROPERTIES_DIR)


def generate_time_windows(start: datetime.date, size: int, end: datetime.date = None) -> iter:
    """Generate a collection of time ranges of a certain size"""

    # Default end time range today
    if end is None:
        end = datetime.date.today()

    # Enforce data types
    start = datetime.date(start.year, start.month, start.day)
    end = datetime.date(end.year, end.month, end.day)

    # Define time window size e.g. 15 days
    window = datetime.timedelta(days=size)

    _start = start
    while True:
        _end = _start + window

        # Maximum date
        _end = min(_end, end)

        yield (_start, _end)

        if _end >= end:
            return

        # Define start of next
        _start = _end + datetime.timedelta(days=1)


def discover() -> singer.Catalog:
    """Discover catalog of schemas ie. reporting cube definitions"""

    raw_schemas = load_schemas()
    metadata = load_metadata()
    key_properties = load_key_properties()

    streams = list()

    for schema_name, schema in raw_schemas.items():
        stream_metadata = list()
        stream_key_properties = list()

        # Append metadata (if exists)
        stream_metadata.extend(metadata.get(schema_name, list()))
        stream_key_properties.extend(key_properties.get(schema_name, list()))

        # create and add catalog entry
        catalog_entry = singer.catalog.CatalogEntry()
        catalog_entry.stream = schema_name
        catalog_entry.tap_stream_id = schema_name
        catalog_entry.schema = schema
        catalog_entry.metadata = stream_metadata
        catalog_entry.key_properties = stream_key_properties
        streams.append(catalog_entry)

    return singer.Catalog(streams)


def get_selected_streams(catalog: singer.Catalog) -> list:
    """
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and data with a 'selected' entry
    """
    selected_streams = list()

    for stream in catalog.streams:
        stream_metadata = singer.metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if singer.metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams


def build_report_definition(config: dict, stream, start_date: datetime.date,
                            end_date: datetime.date) -> dict:
    """
    Convert a JSON schema to a Gemini report request

    JSON schema:

        http://json-schema.org/
    """
    # Check type
    for date in {start_date, end_date}:
        if not isinstance(date, datetime.date):
            raise TypeError(type(date))

    return report.GeminiReport.build_definition(
        advertiser_ids=list(config['advertiser_ids']),
        cube=str(stream.stream),
        field_names=list(stream.schema.properties.keys()),
        start_date=start_date,
        end_date=end_date,
    )


def filter_schema(schema: dict, metadata: list) -> dict:
    """Select fields using meta-data"""
    for item in metadata:
        try:
            if item['breadcrumb'][0] == 'properties':
                property_name = item['breadcrumb'][1]

                # Get metadata for this property
                selected = item['metadata'].get('selected', True)
                inclusion = item['metadata'].get('inclusion', 'available')

                # Some fields are mandatory
                if inclusion == 'automatic':
                    selected = True

                # Remove if not selected
                if not selected or (inclusion == 'unsupported'):
                    del schema.properties[property_name]

                    singer.log_debug('Removed property "%s"', property_name)

        except IndexError:
            pass

    return schema


def sync(config: dict, state: dict, catalog: singer.Catalog):
    """Synchronise data from source schemas using input context"""

    # Parse date
    start_date = datetime.datetime.fromisoformat(config['start_date'])
    start_date = start_date.date()

    # Load state
    try:
        assert state['type'] == 'STATE', 'Invalid state file'

        # Begin where we left off
        start_date = datetime.date.fromisoformat(state['value'])
    except KeyError:
        pass

    selected_stream_ids = get_selected_streams(catalog)

    if not selected_stream_ids:
        singer.log_warning('No streams selected')

    # Loop over streams in catalog
    for stream in catalog.streams:

        stream_id = stream.tap_stream_id

        # Skip if not selected for sync
        if stream_id not in selected_stream_ids:
            continue

        singer.log_info('Syncing stream:%s', stream_id)

        filter_schema(stream.schema, stream.metadata)

        # Emit schema
        singer.write_schema(stream_name=stream_id, schema=stream.schema.to_dict(),
                            key_properties=stream.key_properties)

        # Initialise Gemini HTTP API session
        session = transport.GeminiSession(
            client_id=config['username'],
            api_version=config['api_version'],
            client_secret=config['password'],
            refresh_token=config['refresh_token'],
            user_agent=config['user_agent'],
            session_options=config.get('session', dict())
        )

        time_extracted = singer.utils.now()

        # Create data stream
        if stream_id in OBJECT_MAP.keys():
            # List API objects
            model = OBJECT_MAP[stream_id]

            # Write records
            for data in model.list(session=session):
                record = singer.transform(data=data, schema=stream.schema,
                                          metadata=stream.metadata)

                singer.write_record(
                    stream_name=stream_id,
                    record=record,
                    time_extracted=time_extracted
                )

        else:
            # Run report

            # Define time range
            # Maximum look back (i.e. earliest start date for report)
            try:
                days = MAX_LOOK_BACK_DAYS[stream_id]
                start_date = max(start_date, datetime.date.today() - datetime.timedelta(days=days))
                singer.log_warning("%s enforced maximum look back of %s days, start date set to %s",
                                   stream_id, days, start_date)
            except KeyError:
                pass

            end_date = datetime.date.today()

            # Break into time window chunks
            try:
                time_windows = generate_time_windows(start=start_date,
                                                     size=MAX_WINDOW_DAYS[stream_id])
            except KeyError:
                time_windows = (
                    (start_date, end_date),
                )

            for start, end in time_windows:
                # Build report definition
                report_definition = build_report_definition(
                    config=config,
                    stream=stream,
                    start_date=start,
                    end_date=end
                )

                # Define the report request
                rep = report.GeminiReport(
                    session=session,
                    report_definition=report_definition,
                    poll_interval=config.get('poll_interval', 1)
                )

                # Emit records

                # Stream data rows
                with singer.metrics.Timer(metric='job_timer', tags=rep.tags):
                    # Generate data and count rows
                    with singer.metrics.Counter(metric='record_count', tags=rep.tags) as counter:
                        for data in rep.stream():
                            record = singer.transform(data=data, schema=stream.schema,
                                                      metadata=stream.metadata)
                            singer.write_record(
                                stream_name=stream_id,
                                record=record,
                                time_extracted=time_extracted
                            )
                            counter.increment()

                # Save state on success
                singer.write_state(value=rep.end_date)


@singer.utils.handle_top_exception(LOGGER)
def main():
    """Run tap"""

    # Parse command line arguments
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog.to_dict(), indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        if not isinstance(catalog, singer.Catalog):
            raise ValueError('Catalogue is not of type singer.Catalog')

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
