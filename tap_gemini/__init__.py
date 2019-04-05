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

import singer.metadata
import singer.utils

import tap_gemini.report
import tap_gemini.transport

__version__ = '0.0.1'

REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
LOGGER = singer.get_logger()

# Schema config
SCHEMAS_DIR = 'schemas'
METADATA_DIR = 'metadata'
KEY_PROPERTIES_DIR = 'key_properties'


def get_abs_path(path: str) -> str:
    """Build the absolute path on the local filesystem"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_config(dir_path: str) -> dict:
    """Load all config files from the specified directory"""

    abs_path = get_abs_path(dir_path)

    schemas = dict()

    # Iterate over schema files
    for filename in os.listdir(abs_path):
        path = os.path.join(abs_path, filename)
        file_raw = filename.replace('.json', '')

        # Parse JSON
        with open(path) as file:
            schemas[file_raw] = json.load(file)

            LOGGER.info('Loaded "%s"', file.name)

    return schemas


def load_schemas() -> dict:
    """Load schemas from config files"""

    return load_config(SCHEMAS_DIR)


def load_metadata() -> dict:
    """Load metadata from config files"""

    return load_config(METADATA_DIR)


def load_key_properties() -> dict:
    """Load key properties from config files"""

    return load_config(KEY_PROPERTIES_DIR)


def discover() -> dict:
    """Discover catalog of schemas ie. reporting cube definitions"""

    LOGGER.info('Discovering schemas...')

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
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': stream_metadata,
            'key_properties': stream_key_properties
        }
        streams.append(catalog_entry)

    return dict(streams=streams)


def get_selected_streams(catalog: dict) -> list:
    """
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and data with a 'selected' entry
    """
    selected_streams = list()

    for stream in catalog['streams']:
        stream_metadata = singer.metadata.to_map(stream['metadata'])
        # stream metadata will have an empty breadcrumb
        if singer.metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream['tap_stream_id'])

    return selected_streams


def sync(config: dict, state: dict, catalog: dict):
    """Synchronise data from source schemas using input context"""

    # Parse date
    start_date = datetime.datetime.fromisoformat(config['start_date'])

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog['streams']:

        stream_id = stream['tap_stream_id']

        # Skip if not selected for sync
        if stream_id not in selected_stream_ids:
            continue

        LOGGER.info('Syncing stream:%s', stream_id)

        # Emit schema
        singer.write_schema(
            stream_name=stream_id,
            schema=stream['schema'],
            key_properties=stream['key_properties']
        )

        # Create data stream

        report_definition = tap_gemini.report.build_report_definition(
            config=config,
            state=state,
            stream=stream,
            start_date=start_date,
            end_date=datetime.date.today())

        # Initialise Gemini HTTP API session
        session = tap_gemini.transport.GeminiSession(
            client_id=config['username'],
            api_version=config['api_version'],
            access_token=config['password'],
            user_agent=config['user_agent']
        )

        gemini_report = tap_gemini.report.GeminiReport(
            session=session,
            report_definition=report_definition,
            poll_interval=config['poll_interval']
        )

        # Emit records
        for record in gemini_report.run():
            singer.write_record(stream_name=stream_id, record=record)


@singer.utils.handle_top_exception(LOGGER)
def main():
    """Run tap"""

    # Parse command line arguments
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        # Ensure data type is dictionary
        try:
            catalog = catalog.to_dict()
        except AttributeError:
            pass

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
