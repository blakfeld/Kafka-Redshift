"""
exceptions.py:
    Custom exceptions for use by `kafka_redshift`.

Author: Corwin Brown <corwin@corwinbrown.com>
"""
from __future__ import print_function, absolute_import


class KafkaRedshiftException(Exception):
    """
    Generic exception for KafkaRedhisft to raise.
    """
    pass


class BadConfigException(KafkaRedshiftException):
    """
    Exception to raise when an error is encountered
        parsing/loading/finding a config file.
    """
    pass


class AuthenticationException(KafkaRedshiftException):
    """
    Exception to raise if we have issues authenticating.
    """
    pass


class MissingAvroToolsException(KafkaRedshiftException):
    """
    Exception to raise if the avro tools package is missing.
    """
    pass


class UnableToParseAvroException(KafkaRedshiftException):
    """
    Exception to raise if we can't parse the provided avro
        schema/protocol.
    """
    pass
