"""
Utility to copy data from a Kafka topic to a Redshift database.

Usage:
    kafka-redshift [options]

Options:
    --config-file=<PATH>        Path to a config file.
    --kafka-topic=<TOPIC>       Kafka topic to consume.
    --kafka-hosts=<SERVERS>     Comma separated list of Kafka nodes.
    --redshift-host=<HOST>      Redshift host to connect to.
    --redshift-dbname=<DBNAME>  Name of Redshift database to use.
    --redshift-user=<USER>      User to connect to Redshift as.
    --redshift-pass=<PASS>      Password to use when connecting to Redshift.
    --redshift-port=<PORT>      Port to connect to Redshift with.
    --avro-protocol=<PATH>      Path to Avro Protocol file.
    --schema-name=<NAME>        Schema name to use from the protocol
    --s3-bucket=<BUCKET>        S3 Bucket to store data in.
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


class Config(object):
    """
    Class to help reconcile multiple potential configuration sources.
    """

    def __init__(self, config_fpath, cli_args, defaults):
        """
        Constructor.

        Args:
            config_fpath (str):     Path to config file.
            cli_args (dict):        DocOpt generated args.
            defaults (obj):         Defaults obj.
        """
        self.config = self.read_config_file(config_fpath)
        self.cli_args = cli_args
        self.defaults = defaults

    def get(self, key):
        """
        Take a CLI argument, and attempt to resolve it using the
            following order:

        1. CLI Args
        2. Config File
        3. Defaults

        Args:
            cli_key (str):      The CLI arg to resolve.
        """
        # DocOpt populates all expected keys with Nonetypes, so
        #   we have to test for existence of value, not key.
        value = self.cli_args.get(key)
        if value:
            return value

        # If we didn't find what we wanted in args, munge the key to be
        #   like something we'd expect in our config file.
        key = key.replace('--', '').replace('-', '_')
        try:
            return self.config[key]
        except KeyError:
            pass

        # if we still don't have anything, shift to uppercase and check
        #   inside defaults.
        try:
            return getattr(defaults, key.upper())
        except AttributeError:
            raise errors.BadConfigException(
                'Unable to find value for key: {0}'.format(key))

    @staticmethod
    def read_config_file(config_fpath):
        """
        Reads a provided config file and returns the contents.

        Args:
            config_file (str):      Path of the config file to read.

        Returns:
            dict

        Raises:
            BadConfigException if PyYaml is unable to parse the file.
        """
        with open(config_fpath, 'r') as config_handle:
            config_data = config_handle.read()

        try:
            config_data = yaml.load(config_data)
        except yaml.YAMLError as e:
            raise errors.BadConfigException(
                'Unable to parse config file located at "{0}"! Error: {1}'
                .format(config_fpath, e))

        return config_data


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
    config = Config(config_file, args, defaults)
    logging.debug('Config file at path "%s" successfully loaded!', config_file)

    kafka_hosts = config.get('--kafka-hosts')
    if isinstance(kafka_hosts, str):
        kafka_hosts = kafka_hosts.split(',')

    kafka_topic = config.get('--kafka-topic')
    rs_host = config.get('--redshift-host')
    rs_dbname = config.get('--redshift-dbname')
    rs_port = config.get('--redshift-port')
    rs_username, rs_password = get_credentials(username=args.get('--redshift-user'),
                                               password=args.get('--redshift-pass'))
    protocol_path = os.path.expanduser(config.get('--avro-protocol'))
    schema_name = config.get('--schema-name')
    s3_bucket = config.get('--s3-bucket')

    kafka_redshift = KafkaRedshift(
        kafka_topic=kafka_topic,
        kafka_hosts=kafka_hosts,
        avro_protocol_path=protocol_path,
        schema_name=schema_name,
        s3_bucket=s3_bucket,
        redshift_host=rs_host,
        redshift_dbname=rs_dbname,
        redshift_user=rs_username,
        redshift_pass=rs_password,
        redshift_port=rs_port,
    )

    kafka_redshift.run()


if __name__ == '__main__':
    sys.exit(main())
