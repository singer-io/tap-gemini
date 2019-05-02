# coding=utf-8
"""
Un-select all tables
"""

import os
import json

PATH = "C:\\Users\\joe.heffer\\Documents\Python\\tap-gemini\\tap_gemini\\metadata"
DIR = 'metadata'
INDENT = 2


def main():
    os.makedirs(DIR, exist_ok=True)

    for filename in os.listdir(PATH):
        path = os.path.join(PATH, filename)
        with open(path) as file:

            metadata = json.load(file)
            print('Loaded', file.name)

        # Set 'selected' to false on the main item
        for i, item in enumerate(metadata):
            if not item['breadcrumb']:
                item['metadata']['selected'] = False
                metadata[i] = item

        output_path = os.path.join(DIR, filename)
        with open(output_path, 'w') as file:
            json.dump(metadata, file, indent=INDENT)
            print('Wrote', output_path)


if __name__ == '__main__':
    main()
