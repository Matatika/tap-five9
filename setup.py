#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-five9",
    version="1.0.4",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_five9"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer_sdk",
        "singer-python==1.8.1",
        "requests",
        "five9",
        "inflection",
        "zeep",
        "pendulum"
    ],
    entry_points="""
    [console_scripts]
    tap-five9=tap_five9:main
    """,
    packages=["tap_five9"],
    package_data = {
        "schemas": ["tap_five9/schemas/*.json"]
    },
    include_package_data=True,
)
