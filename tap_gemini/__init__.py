#!/usr/bin/env python3
# coding=utf-8
"""
Singer Tap for Yahoo Gemini

Gemini API documentation:

    - https://developer.yahoo.com/nativeandsearch/guide/

Singer:

    - All timestamps must use the RFC3339 standard.
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
    adgroup=tap_gemini.api.AdGroup,
    ad=tap_gemini.api.Ad,
    keyword=tap_gemini.api.Keyword,
    targetingattribute=tap_gemini.api.TargetingAttribute,
    adextensions=tap_gemini.api.AdExtensions,
    sharedsitelink=tap_gemini.api.SharedSitelink,
    sharedsitelinksetting=tap_gemini.api.SharedSitelinkSetting,
    adsitesetting=tap_gemini.api.AdSiteSetting,
)


def cast_date_to_datetime(date: datetime.date = None) -> datetime.datetime:
    """
    Convert a date (default: today) to a timezone-aware datetime object

    :param date: Calendar date, defaults to current day
    """

    # Default to current date
    if date is None:
        date = datetime.date.today()

    # Build timezone-aware datetime object
    return datetime.datetime.combine(
        date=date,
        time=datetime.time(0, tzinfo=pytz.UTC)
    )


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
        end = cast_date_to_datetime(date=datetime.date.today())

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
    """
    Discover catalog of schemas ie. reporting cube definitions
    """

    raw_schemas = load_schemas()
    metadata = load_metadata()
    # Disable key properties to avoid file-not-found errors because these aren't used
    #key_properties = load_key_properties()
    key_properties = dict()

    streams = list()

    # Build catalog by iterating over schemas
    for schema_name, schema in raw_schemas.items():
        stream_metadata = list()
        stream_key_properties = list()

        # Append metadata (if exists)
        # TODO Use helper functions
        # TODO https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        stream_metadata.extend(metadata.get(schema_name, list()))
        stream_key_properties.extend(key_properties.get(schema_name, list()))

        # Create catalog entry
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
        end_date = cast_date_to_datetime(date=datetime.date.today())

    return dict(
        cube=str(stream.stream),
        field_names=list(stream.schema.properties.keys()),
        start_date=datetime.date(start_date.year, start_date.month, start_date.day),
        end_date=datetime.date(end_date.year, end_date.month, end_date.day),
    )


def filter_schema(schema: dict, metadata: list) -> dict:
    """
    Select fields using meta-data
    """

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


def get_books_closed(rep: tap_gemini.report.GeminiReport) -> datetime.datetime:
    """
    Get the time when the books are closed i.e. the data become static and will no longer change.

    Only set the bookmark to the date when the books are closed, rather than naively using the
    report end date.
    """

    # Default to start date: if books are closed then re-run the report in the future
    # from this same beginning date.
    bookmark_timestamp = cast_date_to_datetime(rep.start_date)

    # Find when "close of business" occurred (i.e. most recent date for which books
    # are closed, within the time range of the report.)
    check_date = cast_date_to_datetime(rep.end_date)

    # Don't bother starting today, go back to yesterday
    if check_date == cast_date_to_datetime(date=datetime.date.today()):
        check_date -= datetime.timedelta(days=1)

    # Find when books are closed, iterating back through time
    while True:
        books_closed_timestamp = rep.are_books_closed(date=check_date)

        # Successfully found when books closed
        if books_closed_timestamp is not None:
            bookmark_timestamp = books_closed_timestamp
            break

        # Go back through time by one day
        check_date -= datetime.timedelta(days=1)

        # Stop looping
        if check_date < bookmark_timestamp:
            break

    return bookmark_timestamp


def transform_property(value, data_type: str, string_format: str = None):
    """Cast data types, as instructed by the schema"""

    # Missing values are null
    if value is None:
        return

    # Cast data type according to schema
    if data_type == 'string':
        value = str(value)

        # Parse dates
        if string_format == 'date-time':
            value = singer.utils.strptime_to_utc(value).isoformat()

    elif data_type == 'integer':
        value = int(value)

    elif data_type == 'number':
        value = float(value)

    else:
        raise ValueError('Unknown data type', data_type)

    return value


def transform_record(record: dict, schema: dict) -> dict:
    """
    Cast the data types of each field (property) of a record according to the schema definition,
    ready for output using JSON schema.

    :param record: Input record
    :param schema: Schema definition of data types.
    :return: Record with data types safe for output to JSON schema
    """

    # Build a new dictionary, rather than mutating the input dictionary
    transformed_record = dict()

    # Iterate over properties in the record
    for key, value in record.items():

        # Get the property definition from the schema
        prop = schema['properties'][key]

        data_type = prop['type']

        # If multiple data types are defined, pick one at random
        if not isinstance(data_type, str):
            data_types = set(data_type)

            # If we have multiple data types to choose from, exclude null
            if len(data_types) > 1:
                data_types -= {'null'}

            data_type = data_types.pop()

        try:
            value = transform_property(
                value=value,
                data_type=data_type,
                string_format=prop.get('format')
            )

        # Show which property has caused the problem
        except ValueError:
            LOGGER.error('Property "%s" could not be cast to data type "%s"', key, prop['type'])
            raise

        transformed_record[key] = value

    return transformed_record


def write_records(stream: singer.catalog.CatalogEntry, rows: iter, tags=None):
    """
    Wrapper for singer utils
    """

    schema = stream.schema.to_dict()
    # metadata = singer.metadata.to_map(stream.metadata)
    time_extracted = singer.utils.now()

    # Iterate over rows of data
    with singer.metrics.Timer(metric='job_timer', tags=tags):
        with singer.metrics.Counter(metric='record_count', tags=tags) as counter:
            for row in rows:

                # Check type
                if not isinstance(row, dict):
                    raise TypeError('ROW: Type {} is not dict'.format(type(row)))

                # Disabled because this seems to return a string, rather than a dictionary
                # Transform data row for JSON output
                # record = singer.transform(
                #     data=row,
                #     schema=schema,
                #     metadata=metadata
                # )

                record = transform_record(row, schema=schema)

                # Check type
                if not isinstance(record, dict):
                    raise TypeError('RECORD: Type {} is not dict'.format(type(record)))

                # Emit record
                try:

                    singer.write_record(
                        stream_name=stream.tap_stream_id,
                        record=record,
                        time_extracted=time_extracted
                    )

                # Log problems that may occur in the tap after the record is emitted
                except (OSError, BrokenPipeError):
                    LOGGER.error('Tap record parsing error for stream "%s"', stream.tap_stream_id)
                    LOGGER.error('Problematic record: "%s"', json.dumps(record))
                    raise

                counter.increment()


def sync(config: dict, state: dict, catalog: singer.Catalog):
    """
    Synchronise data from source schemas using input context
    """
    
    session = None

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

        LOGGER.info('Syncing stream: "%s"', stream_id)

        filter_schema(stream.schema, stream.metadata)

        # Emit schema
        singer.write_schema(
            stream_name=stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties
        )

        # Initialise Gemini HTTP API session (only do this once)
        if session is None:
            session = tap_gemini.transport.GeminiSession(
                # Mandatory
                client_id=config['username'],
                client_secret=config['password'],
                refresh_token=config['refresh_token'],

                # Optional
                api_version=config.get('api_version'),
                user_agent=config.get('user_agent'),
                session_options=config.get('session', dict()),
                sandbox=config.get('sandbox')
            )
            
            # Get a list of all the account IDs
            advertiser_ids = config.get('advertiser_ids', [adv['id'] for adv in session.advertisers])

        # Create data stream
        if stream_id in OBJECT_MAP.keys():

            # List API objects
            model = OBJECT_MAP[stream_id]
            write_records(
                stream=stream,
                rows=model.list_data(session=session),
                tags=dict(
                    object=stream_id
                )
            )

        else:
            # Run report

            # Use bookmark to continue where we left off
            bookmark = bookmarks.get(stream_id, dict())
            start_date = bookmark.get(tap_gemini.settings.BOOKMARK_KEY, start_date)

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

            # Break into time window chunks, if necessary
            try:
                time_windows = generate_time_windows(
                    start=start_date,
                    size=tap_gemini.settings.MAX_WINDOW_DAYS[stream_id]
                )
            except KeyError:
                # Default time window: just use specified start/end date
                time_windows = (
                    (start_date, cast_date_to_datetime(date=datetime.date.today())),
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

                report_params['advertiser_ids'] = advertiser_ids

                # Define the report
                rep = tap_gemini.report.GeminiReport(
                    session=session,
                    poll_interval=config.get('poll_interval'),
                    **report_params
                )

                # Emit records
                write_records(
                    stream=stream,
                    rows=rep.stream(),
                    tags=rep.tags
                )

                # Bookmark the progress through the stream
                # Get the time when the data is complete (no further changes will occur)
                bookmark_timestamp = get_books_closed(rep=rep)

                # Preserve state for each stream
                singer.write_bookmark(
                    state=state,
                    tap_stream_id=stream_id,
                    key=tap_gemini.settings.BOOKMARK_KEY,
                    val=cast_date_to_datetime(bookmark_timestamp).isoformat()
                )

                singer.write_state(state)


@singer.utils.handle_top_exception(LOGGER)
def main():
    """
    Execute tap: build catalog and synchronise
    """

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
            raise TypeError('Type is not singer.Catalog')

        sync(args.config, args.state, catalog)


if __name__ == '__main__':
    main()
