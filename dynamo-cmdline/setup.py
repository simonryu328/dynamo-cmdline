# -*- coding: utf-8 -*-


"""setup.py: setuptools control."""


import re
from setuptools import setup


version = re.search(
    '^__version__\s*=\s*"(.*)"',
    open('dynamo/cli.py').read(),
    re.M
    ).group(1)


with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")


setup(
    name = "dynamo-cmdline",
    packages = ["dynamo"],
    entry_points = {
        "console_scripts": ['dynamo = dynamo.cli:main']
        },
    version = version,
    description = "Python command line interface for copying/querying/restoring DynamoDB tables and items.",
    long_description = long_descr,
    long_description_content_type='text/markdown',
    author = "Simon Ryu",
    author_email = "simonryu328@gmail.com",
    url = "https://github.com/ecoation/root/cmdline-dynamo",
    )
