"""
Utility to copy data from a Kafka topic to a Redshift database.

Usage:
    kafka-redshift [OPTIONS]

OPTIONS:
    --config-file=<PATH>        Path to a config file.
    --kafka-topic=<TOPIC>       Kafka topic to consume.
    --kafka-hosts=<SERVERS>     Comma separated list of Kafka nodes.
    --redshift-host=<HOST>      Redshift host to connect to.
    --redshift-dbname=<DBNAME>  Name of Redshift database to use.
    --redshift-user=<USER>      User to connect to Redshift as.
    --redshift-pass=<PASS>      Password to use when connecting to Redshift.
    --redshift-port=<PORT>      Port to connect to Redshift with.
    --avro-schema=<PATH>        Path to Avro Schema file.
    -v --verbose                Toggle verbose mode.
    -h --help                   Display this message.
"""
from __future__ import print_function, absolute_import

import logging
import os
import sys
from getpass import getpass

import yaml
from docopt import docopt

import defaults
import errors
from kafka_redshift import KafkaRedshift


def find_config_file(config_search_paths):
    """
    Search a list of provided paths for a config file that both
        exists and that we have permission to read.

    Args:
        config_search_paths (list):     List of paths to search
                                            for a config file.

    Returns:
        str

    Raises:
        BadConfigException if a config file is not found.
    """
    for config_path in config_search_paths:
        if os.path.isfile(config_path) and os.access(config_path, os.R_OK):
            return config_path

    raise errors.BadConfigException(
        'Unable to find a config file in the following search path: {0}'
        .format(config_search_paths))


def read_config_file(config_file):
    """
    Reads a provided config file and returns the contents.

    Args:
        config_file (str):      Path of the config file to read.

    Returns:
        dict

    Raises:
        BadConfigException if PyYaml is unable to parse the file.
    """
    with open(config_file, 'r') as config_handle:
        config_data = config_handle.read()

    try:
        config_data = yaml.load(config_data)
    except yaml.YAMLError as e:
        raise errors.BadConfigException(
            'Unable to parse config file located at "{0}"! Error: {1}'
            .format(config_file, e))

    return config_data


def get_config_key(args, config, key):
    """
    Given a config dict, search for a key. If that key does not exist
        return that value from our defaults module.

    Args:
        args (dict):        DocOpt generaetd dict.
        config (dict):      Dict containing config information.
        key (str):          Key to search for in the dict.

    Returns:
        Desired value wither from config or defaults.py.
    """
    if key in args:
        return args[key]

    # If we didn't find what we wanted in args, munge the key to be
    #   like something we'd expect in our config file.
    key = key.replace('--', '').replace('-', '_')
    if key in config:
        return config[key]

    # if we still don't have anything, shift to uppercase and check
    #   inside defaults.
    try:
        return getattr(defaults, key.upper())
    except AttributeError:
        raise errors.BadConfigException(
            'Unable to find value for key: {0}'.format(key))


def get_credentials(username=None, password=None):
    """
    Get credentials. First attempt to use CLI provided creds, then
        search the environment.

    Args:
        username (str):     Username provided on the CLI.
        password (str):     Password provided on the CLI.

    Returns:
        Tuple:  (username, password)

    Raises:
        AuthenticationException if we're unable to find any
            credentials.
    """
    if username:
        logging.debug('Found CLI provided username.')
        if not password:
            logging.debug('No password provided! Prompting...')
            password = getpass('Password: ')
    else:
        logging.debug('Searching environment for credentials...')
        username = os.environ.get(defaults.USERNAME_ENV_VAR)
        password = os.environ.get(defaults.PASSWORD_ENV_VAR)

    if not all([username, password]):
        raise errors.AuthenticationException(
            'Unable to find username and password!')

    return username, password


def main():
    """
    Main.
    """
    args = docopt(__doc__)

    log_level = logging.DEBUG if args.get('--verbose') else logging.ERROR
    logging.basicConfig(log_level=log_level)

    logging.debug('Searching for config file...')
    config_file = args.get('--config-file') or find_config_file(defaults.CONFIG_SEARCH_PATHS)
    config = read_config_file(config_file)
    logging.debug('Config file at path "%s" successfully loaded!', config_file)

    kafka_hosts = get_config_key(args, config, '--kafka-hosts')
    if isinstance(kafka_hosts, str):
        kafka_hosts = kafka_hosts.split(',')

    kafka_topic = get_config_key(args, config, '--kafka-topic')
    rs_host = get_config_key(args, config, '--redshift-host')
    rs_dbname = get_config_key(args, config, '--redshift-dbname')
    rs_port = get_config_key(args, config, '--redshift-port')
    rs_username, rs_password = get_credentials(username=args.get('--username'),
                                               password=args.get('--password'))
    schema_path = get_config_key(args, config, '--avro-schema')

    kafka_redshift = KafkaRedshift(
        kafka_topic=kafka_topic,
        kafka_hosts=kafka_hosts,
        avro_schema_path=schema_path,
        redshift_host=rs_host,
        redshift_dbname=rs_dbname,
        redshift_user=rs_username,
        redshift_pass=rs_password,
        refshift_port=int(rs_port),
    )

    kafka_redshift.run()


if __name__ == '__main__':
    sys.exit(main())
