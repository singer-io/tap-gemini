"""
Scrape Yahoo Gemini report info and generate JSON schemas using guesswork.
"""

import logging
import json
import os

from pprint import pprint

import pandas as pd

LOGGER = logging.getLogger(__name__)

PATH = "cubes.html"
SCHEMA_DIR = 'schemas'
METADATA_DIR = 'metadata'

CUBES = [
    'performance_stats',
    'slot_performance_stats',
    'site_performance_stats',
    'campaign_bid_performance_stats',
    'structured_snippet_extension_stats',
    'product_ad_performance_stats',
    'adjustment_stats',
    'keyword_stats',
    'search_stats',
    'ad_extension_details',
    'call_extension_stats',
    'user_stats',
    'product_ads',
    'conversion_rules_stats',
    'domain_performance_stats',
]

SCHEMA = {
    "type": [
        "date-time",
        "date",
        "integer",
        "float",
        "string"
    ],
    "additionalProperties": False,
    "properties": dict()
}

METADATA = [
    {
        "metadata": {
            "selected": True,
            "inclusion": "available",
            "replication_method": "INCREMENTAL"
        },
        "breadcrumb": []
    },
    {
        "metadata": {
            "inclusion": "automatic"
        },
        "breadcrumb": [
            "properties",
            "Day"
        ]
    },
    {
        "metadata": {
            "inclusion": "automatic"
        },
        "breadcrumb": [
            "properties",
            "Advertiser ID"
        ]
    }]


def guess_type(row: dict) -> str:
    if row['Field'] == 'Day':
        return 'date'

    if 'CPC' in row['Field']:
        return 'float'

    if row['Field'].endswith('ID'):
        return 'integer'

    if row['Field'].endswith('Rate'):
        return 'float'

    if 'cost' in row['Description']:
        return 'float'
    if 'spend' in row['Description']:
        return 'float'

    if 'rate' in row['Description']:
        return 'float'

    if row['Type'][0].casefold() == 'd':
        return 'string'

    return 'integer'


def build_properties(df) -> dict:
    props = dict()

    for _, row in df.iterrows():
        prop = dict(
            type=guess_type(row),
            description=row['Description'].strip()
        )

        props[row['Field']] = prop

    return props


def serialise(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as file:
        json.dump(obj, file, indent=2)
        LOGGER.info('Wrote "%s"', file.name)


def main():
    logging.basicConfig(level=logging.INFO)

    with open(PATH) as file:
        data = pd.read_html(file)
        LOGGER.info('Read "%s"', file.name)

    for name, df in zip(CUBES, data):
        df['Description'].fillna('', inplace=True)

        schema = SCHEMA.copy()
        schema['properties'] = build_properties(df)

        # Save schema file
        filename = "{}.json".format(name)
        path = os.path.join(SCHEMA_DIR, filename)
        serialise(path, schema)

        # Save metadata file
        metadata = METADATA.copy()
        filename = "{}.json".format(name)
        path = os.path.join(METADATA_DIR, filename)
        serialise(path, metadata)


if __name__ == '__main__':
    main()
