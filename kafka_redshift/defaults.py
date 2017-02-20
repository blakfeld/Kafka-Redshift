"""
defaults.py:
    Default values to use for kafka_redshift.

Author: Corwin Brown <corwin@corwinbrown.com>
"""
from __future__ import print_function, absolute_import

import os

PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))

CONFIG_SEARCH_PATHS = [
    os.path.join(os.path.expanduser('~'), 'kafka_redshift.yaml'),
    os.path.join('etc', 'kafka_redshift', 'config.yaml'),
    os.path.join(PROJECT_PATH, 'conf', 'config.yaml')
]
USERNAME_ENV_VAR = 'KAFKA_REDSHIFT_USERNAME'
PASSWORD_ENV_VAR = 'KAFKA_REDSHIFT_PASSWORD'
