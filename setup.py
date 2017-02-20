#!/usr/bin/env python
"""
setup.py

Author: Corwin Brown <corwin@corwinbrown.com>
"""

import os
from setuptools import setup, find_packages

MY_PATH = os.path.abspath(os.path.dirname(__file__))


def get_long_description(fpath=None):
    """
    Read our README file. For use with "long_description".

    Args:
        fpath (str):    File path of readme.

    Returns:
        str
    """
    fpath = fpath or os.path.join(MY_PATH, 'README.md')
    with open(fpath, 'r') as handle:
        return handle.read()


def get_requirements(fpath=None):
    """
    Read our "requirements.txt" file and return a list of required
        packages.

    Args:
        fpath (str):    File path of requirements.txt

    Returns:
        str
    """
    fpath = fpath or os.path.join(MY_PATH, 'requirements.txt')

    requirements = []
    with open(fpath, 'r') as handle:
        requirements = handle.readlines()

    return [r.split('==')[0] for r in requirements]


setup(
    name='waldo_kafka_consumer',
    version='1.0.0',
    description='',
    long_description=get_long_description(),
    url='https://github.com/blakfeld',
    author='Corwin Brown',
    author_email='corwin@corwinbrown.com',
    license='MIT',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=get_requirements(),
)
