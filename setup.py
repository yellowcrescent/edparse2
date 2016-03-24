#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages
setup(
    name = "edparse2",
    version = "0.20.2",
    author = "Jacob Hipps",
    author_email = "jacob@ycnrg.org",
    license = "MIT",
    description = "Corpus parser, scraper, data conversion and statistical analysis tool for hotarun.co",
    keywords = "scraper parser edrdg japanese kanji dictionary corpus",
    url = "https://bitbucket.org/yellowcrescent/edparse2",

    packages = find_packages(),
    scripts = ['edparse2'],

    install_requires = ['docutils>=0.3','setproctitle','pymongo>=3.0','redis>=2.10','py2neo>=2.0.8','MySQL-python>=1.2.5','psycopg2>=2.4.5','BeautifulSoup4>=4.4.1','lxml>=3.5.0','requests>=2.2.1','mecab-python3>=0.7'],

    package_data = {
        '': [ '*.md' ],
    }

    # could also include long_description, download_url, classifiers, etc.
)
