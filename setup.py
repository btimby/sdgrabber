#!/bin/env python

from distutils.core import setup

name = 'pysd'
version = '0.1'

with open('LICENSE', 'r') as l:
    license = l.read()

setup(
    name = name,
    version = version,
    description = 'Schedules Direct client for Python',
    long_description = 'Implements API client for Schedules Direct.' \
                       ' Can also save xmltv.xml file.',
    author = 'Ben Timby',
    author_email = 'btimby@gmail.com',
    url = 'http://github.com/btimby/' + name + '/',
    license = license,
    packages = ["pysd"],
    classifiers = [
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
