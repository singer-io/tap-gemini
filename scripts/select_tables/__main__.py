# coding=utf-8
"""
Select/deselect tables
"""

import os
import json
import argparse
import logging

LOGGER = logging.getLogger(__name__)

# Options
HOME = os.path.expanduser('~')
PATH = os.path.join(HOME, 'Documents/Python/tap-gemini/tap_gemini/metadata')
DIR = 'metadata'
INDENT = 2


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--select', action='store_true', help='Select tables (default: deselect '
                                                              'tables)')
    args = parser.parse_args()

    os.makedirs(DIR, exist_ok=True)

    for filename in os.listdir(PATH):
        path = os.path.join(PATH, filename)
        with open(path) as file:

            metadata = json.load(file)
            LOGGER.info('Loaded "{}"'.format(file.name))

        # Select/deselect this item
        for i, item in enumerate(metadata):
            if not item['breadcrumb']:
                item['metadata']['selected'] = args.select
                metadata[i] = item

        output_path = os.path.join(DIR, filename)
        with open(output_path, 'w') as file:
            json.dump(metadata, file, indent=INDENT)
            LOGGER.info('Wrote "{}"'.format(file.name))


if __name__ == '__main__':
    main()
