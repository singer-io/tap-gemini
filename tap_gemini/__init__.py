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

import pytz
import singer
import singer.metadata
import singer.utils

import tap_gemini.api
import tap_gemini.report
import tap_gemini.transport
import tap_gemini.settings

LOGGER = singer.get_logger()

# Map schema name to API object
OBJECT_MAP = dict(
    advertiser=tap_gemini.api.Advertiser,
    campaign=tap_gemini.api.Campaign,
    # TODO Implement further objects
    # adgroup=api.AdGroup
)


def cast_date_to_datetime(date: datetime.date) -> datetime.datetime:
    """Convert a date (default: today) to a timezone-aware datetime object"""

    # Build timezone-aware datetime object
    return datetime.datetime.combine(
        date=date,
        time=datetime.time(0, tzinfo=pytz.UTC)
    )


TODAY = cast_date_to_datetime(date=datetime.date.today())


def get_abs_path(path: str) -> str:
    """Build the absolute path on the local filesystem"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_directory(dir_path: str) -> dict:
    """Load all configuration files in the specified directory"""

    abs_path = get_abs_path(dir_path)

    data = dict()

    # Iterate over JSON files
    for filename in os.listdir(abs_path):
        if not filename.endswith('json'):
            continue

        path = os.path.join(abs_path, filename)
        name = filename.replace('.json', '').casefold().replace(' ', '_')

        # Parse JSON
        with open(path) as file:
            try:
                data[name] = json.load(file)
            except json.JSONDecodeError:
                singer.log_error('JSON syntax error in file "%s"', file.name)
                raise

            singer.log_debug('Loaded "%s"', file.name)

    return data


def load_schemas() -> dict:
    """Load schemas from config files"""

    schemas = load_directory(tap_gemini.settings.SCHEMAS_DIR)

    # Build singer.Schema objects from raw JSON data
    for name, data in schemas.items():
        schemas[name] = singer.Schema.from_dict(data=data)

    return schemas


def load_metadata() -> dict:
    """Load metadata from config files"""

    return load_directory(tap_gemini.settings.METADATA_DIR)


def load_key_properties() -> dict:
    """Load key properties from config files"""

    return load_directory(tap_gemini.settings.KEY_PROPERTIES_DIR)


def generate_time_windows(start: datetime.date, size: int, end: datetime.date = None) -> iter:
    """
    Generate a collection of time ranges of a certain size to overcome the window-size limits for
    some reports.

    Each time range is defined using a two-tuple that contains the start and end date of that time
    window.

    :rtype: iter[tuple[datetime.date]]
    """

    # Default end time range today
    if end is None:
        end = TODAY

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

        # Move to the start of the next window
        _start = _end + datetime.timedelta(days=1)


def discover() -> singer.Catalog:
    """Discover catalog of schemas ie. reporting cube definitions"""

    raw_schemas = load_schemas()
    metadata = load_metadata()
    key_properties = load_key_properties()

    streams = list()

    # Build catalog by iterating over schemas
    for schema_name, schema in raw_schemas.items():
        stream_metadata = list()
        stream_key_properties = list()

        # Append metadata (if exists)
        stream_metadata.extend(metadata.get(schema_name, list()))
        stream_key_properties.extend(key_properties.get(schema_name, list()))

        # Create and add catalog entry
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


def build_report_params(config: dict, stream, start_date: datetime.datetime,
                        end_date: datetime.datetime = None) -> dict:
    """
    Convert a JSON schema to Gemini report parameters.

    JSON schema: http://json-schema.org/
    """

    if end_date is None:
        end_date = TODAY

    return dict(
        advertiser_ids=list(config['advertiser_ids']),
        cube=str(stream.stream),
        field_names=list(stream.schema.properties.keys()),
        start_date=datetime.date(start_date.year, start_date.month, start_date.day),
        end_date=datetime.date(end_date.year, end_date.month, end_date.day),
    )


def get_books_closed(rep: tap_gemini.report.GeminiReport) -> datetime.datetime:
    """
    Get the time when the books are closed i.e. the data become static and will no longer change.

    Only set the bookmark to the date when the books are closed, rather than naively using the
    report end date.
    """

    # Default to start date: if books are closed then re-run the report in the future
    # from this same beginning date.
    bookmark_timestamp = cast_date_to_datetime(rep.start_date)
    bookmark_date = bookmark_timestamp.date()

    # Find when "close of business" occurred (i.e. most recent date for which books
    # are closed, within the time range of the report.)
    check_date = cast_date_to_datetime(rep.end_date)

    # Find when books are closed, iterating back through time
    while True:
        try:
            books_status = rep.close_of_business(date=check_date)

        except tap_gemini.report.BooksClosedNotImplementedError:
            break

        books_closed = books_status['isMonthClosed'] | books_status['isDayClosed']

        LOGGER.info('CLOSE_OF_BUSINESS: %s %s (%s%%)', check_date.date(),
                    books_closed, books_status.get('dayProgressPercent'))

        # Books are closed to the end of the month
        if books_status['isMonthClosed']:
            bookmark_date = check_date.replace(
                day=1,
                month=check_date.month + 1
            )
            LOGGER.info('CLOSE_OF_BUSINESS: Month starting %s is closed',
                        check_date.date().replace(day=1))

        elif books_status['isDayClosed']:
            bookmark_date = check_date.date()

        if books_closed:
            bookmark_timestamp = datetime.datetime.combine(
                date=bookmark_date,
                time=datetime.time(0, tzinfo=pytz.timezone(
                    books_status['advertiserTimezone']))
            )

        check_date -= datetime.timedelta(days=1)

        # Stop looping
        if books_closed | (check_date < bookmark_timestamp):
            break

    return bookmark_timestamp


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


def row_to_json(row: dict) -> dict:
    if not isinstance(row, dict):
        raise TypeError(type(row))

    new_row = dict()
    for key, value in row.items():
        if isinstance(value, (datetime.datetime, datetime.date)):
            value = value.isoformat()

        new_row[key] = value

    return new_row


def transform_record(record: dict, schema: dict) -> dict:
    """
    Cast the data types of each field (property) of a record according to the schema definition.

    :param record: Input record
    :param schema: Schema definition of data types.
    :return:
    """

    # Build a new dictionary, rather than mutating the input dictionary
    transformed_record = dict()

    # Iterate over properties in the record
    for key, value in record.items():

        # Get the property definition from the schema
        prop = schema['properties'][key]

        # Cast data types, as instructed by the schema
        try:
            if prop['type'] == 'string':
                value = str(value)

                # Parse dates
                if prop.get('format') == 'date-time':
                    value = singer.utils.strptime_to_utc(value).isoformat()

            elif prop['type'] == 'integer':
                value = int(value)

            elif prop['type'] == 'number':
                value = float(value)

            else:
                raise ValueError('Unknown data type', prop['type'])

        # Show which property has caused the problem
        except ValueError:
            LOGGER.error('Property "%s" could not be cast to data type "%s"', key, prop['type'])
            raise

        transformed_record[key] = value

    return transformed_record


def write_records(stream: singer.catalog.CatalogEntry, rows: iter, tags=None, limit: int = 0):
    """Wrapper for singer utils"""

    LOGGER.info('Writing records for stream "%s"', stream.tap_stream_id)

    schema = stream.schema.to_dict()
    # metadata = singer.metadata.to_map(stream.metadata)
    time_extracted = singer.utils.now()

    # Peek at the top N rows
    if limit:
        import itertools
        rows = itertools.islice(rows, 0, limit)
        LOGGER.warning('Data limited to %s rows', limit)

    with singer.metrics.Timer(metric='job_timer', tags=tags):
        with singer.metrics.Counter(metric='record_count', tags=tags) as counter:
            for row in rows:

                if not isinstance(row, dict):
                    LOGGER.error(row)
                    raise TypeError('ROW {} is not dict'.format(type(row)))

                # Disabled because this seems to return a string, rather than a dictionary
                # Transform data row for JSON output
                # record = singer.transform(
                #     data=row,
                #     schema=schema,
                #     metadata=metadata
                # )

                record = transform_record(row, schema=schema)

                if not isinstance(record, dict):
                    raise TypeError('RECORD {} is not dict'.format(type(record)))

                # Emit record
                singer.write_record(
                    stream_name=stream.tap_stream_id,
                    record=record,
                    time_extracted=time_extracted
                )

                counter.increment()


def sync(config: dict, state: dict, catalog: singer.Catalog):
    """Synchronise data from source schemas using input context"""

    # Get bookmarks of state of each stream
    bookmarks = state.get('bookmarks', dict())

    # Parse timestamp and convert to date
    start_date = singer.utils.strptime_to_utc(config['start_date'])

    selected_stream_ids = get_selected_streams(catalog)

    if not selected_stream_ids:
        singer.log_warning('No streams selected')

    # Iterate over streams in catalog
    for stream in catalog.streams:

        stream_id = stream.tap_stream_id

        # Skip if not selected for sync
        if stream_id not in selected_stream_ids:
            continue

        singer.log_info('Syncing stream: "%s"', stream_id)

        filter_schema(stream.schema, stream.metadata)

        # Emit schema
        singer.write_schema(
            stream_name=stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties
        )

        # Initialise Gemini HTTP API session
        session = tap_gemini.transport.GeminiSession(
            client_id=config['username'],
            api_version=config['api_version'],
            client_secret=config['password'],
            refresh_token=config['refresh_token'],
            user_agent=config['user_agent'],
            session_options=config.get('session', dict())
        )

        # Create data stream
        if stream_id in OBJECT_MAP.keys():

            # List API objects
            model = OBJECT_MAP[stream_id]

            write_records(
                stream=stream,
                rows=model.list_data(session=session),
                tags=dict(
                    object=stream_id
                ),
                limit=5
            )

        else:
            # Run report

            # Use bookmark to continue where we left off
            bookmark = bookmarks.get(stream_id, dict())
            start_date = bookmark.get('end_date', start_date)

            # Define time range
            try:
                # Is there a maximum look back? (i.e. earliest start date for report)
                days = tap_gemini.settings.MAX_LOOK_BACK_DAYS[stream_id]

                # Get the current timestamp and "look back" the specified number of days
                look_back_start_date = singer.utils.now() - datetime.timedelta(days=days)

                # Must we confine the time range to avoid errors?
                if look_back_start_date > start_date:
                    start_date = look_back_start_date
                    singer.log_warning(
                        "\"%s\" enforced maximum look back of %s days, start date set to %s",
                        stream_id, days, start_date)

            except KeyError:
                pass

            # Break into time window chunks
            try:
                time_windows = generate_time_windows(
                    start=start_date,
                    size=tap_gemini.settings.MAX_WINDOW_DAYS[stream_id]
                )
            except KeyError:
                # Default time window: just use specified start/end date
                time_windows = (
                    (start_date, None),
                )

            # Each report is run within a single time window
            for start, end in time_windows:
                # Build report definition
                report_params = build_report_params(
                    config=config,
                    stream=stream,
                    start_date=start,
                    end_date=end
                )

                # Define the report
                report = tap_gemini.report.GeminiReport(
                    session=session,
                    poll_interval=config.get('poll_interval'),  # default: one second
                    **report_params
                )

                # Emit records
                write_records(
                    stream=stream,
                    rows=report.stream(),
                    tags=report.tags,
                    limit=5
                )

                # Bookmark the progress through the stream
                # Get the time when the data is complete (no further changes will occur)
                bookmark_timestamp = get_books_closed(rep=report)

                # Preserve state for each stream
                singer.write_bookmark(
                    state=state,
                    tap_stream_id=stream_id,
                    key='end_date',
                    val=cast_date_to_datetime(bookmark_timestamp).isoformat()
                )

                singer.write_state(state)


@singer.utils.handle_top_exception(LOGGER)
def main():
    """Execute tap: build catalog and synchronise."""

    # Parse command line arguments
    args = singer.utils.parse_args(tap_gemini.settings.REQUIRED_CONFIG_KEYS)

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


if __name__ == '__main__':
    main()
