#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-gemini",
    version="0.1.4",
    description="Singer.io tap for extracting data from Yahoo Gemini",
    author="Joe Heffer",
    url="https://github.com/singer-io/tap-gemini",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_gemini"],
    install_requires=[
        "singer-python==5.4.1",
        "requests==2.21.0",
        "pytz==2018.4",
    ],
    entry_points="""
    [console_scripts]
    tap-gemini=tap_gemini:main
    """,
    packages=["tap_gemini"],
    package_data={
        "schemas": ["tap_gemini/schemas/*.json"]
    },
    include_package_data=True,
)
