#!/bin/env python

from distutils.core import setup
from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip


NAME = 'sdgrabber'
VERSION = '0.1'


with open('LICENSE', 'r') as f:
    license = f.read()

pipfile = Project(chdir=False).parsed_pipfile


setup(
    name=NAME,
    version=VERSION,
    description='Schedules Direct client for Python',
    long_description='Implements API client for Schedules Direct.'
                     ' Can also save xmltv.xml file.',
    install_requires=convert_deps_to_pip(pipfile['packages'], r=False),
    author='Ben Timby',
    author_email='btimby@gmail.com',
    url='http://github.com/btimby/' + NAME + '/',
    license=license,
    packages=[NAME],
    classifiers=[
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
