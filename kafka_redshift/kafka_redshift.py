"""
kafka_redshift.py:
    Class to consume on a kafka topic and push data back up to a
    AWS Redshift database.

NOTE:
    The ask was to convert the avro files to something Redshift can
    use. The plan was to convert the avro data to JSON since Redshift
    plays nicely with JSON. However, as of 2015 Redshift can consume
    Avro data. So. Lets just skip that step.

https://aws.amazon.com/about-aws/whats-new/2015/07/amazon-redshift-now-supports-avro-ingestion/
https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html#copy-avro

Author: Corwin Brown <corwin@corwinbrown.com>
"""
from __future__ import print_function, absolute_import

from tempfile import NamedTemporaryFile

import avro
import psycopg2
from kafka import KafkaConsumer


class KafkaRedshift(object):
    """
    Kafka Consumer and Redshift copier.
    """

    def __init__(self,
                 kafka_topic,
                 kafka_hosts,
                 avro_schema_path,
                 redshift_host,
                 redshift_dbname,
                 redshift_user,
                 redshift_pass,
                 redshift_port='5439'):
        """
        Constructor.

        kafka_topic (str):      The Kafka topic to consume.
        kafka_hosts (list):     List of Kafka nodes to connect to.
        avro_schema (str):      Path to Avro Schema file.
        redshift_host (str):    Redshift host to copy data to.
        redshift_dbname (str):  Name of the Redshift database to use.
        redshift_user (str):    The user to connect to Redshift as.
        redshift_pass (str):    The password to use when connecting
                                    to Redshift.
        """
        kafka_hosts = kafka_hosts if isinstance(kafka_hosts, list) else [kafka_hosts]

        # Really psycopg? You couldn't wrap this a little better?
        conn_string = (
            'dbname="{0}" port="{1}" user="{2}" password="{3}" host={4}'
            .format(redshift_dbname,
                    redshift_port,
                    redshift_user,
                    redshift_pass,
                    redshift_host))

        self.conn = psycopg2.connect(conn_string)
        self.consumer = KafkaConsumer(kafka_topic,
                                      bootstrap_severs=kafka_hosts)
        self.schema = avro.schema.parse(open(avro_schema_path).read())

    def run(self):
        """
        Main execution loop.
        """
        for msg in self.consumer:
            with open('/Users/blakfeld/Desktop/test.avro') as f:
                self._write_avro(msg, f)

            # with NamedTemporaryFile(buffsize=0) as temp:
            #     self._write_avro(msg, temp)

    def _write_avro(self, avro_data, out_file_handle):
        """
        Compress avro data and write it to disk.

        NOTE:
            Using the "deflate" codec as opposed to something like
            "snappy" because "deflate" actually seems to produce
            smaller files. At least according to this dude:

            https://blog.sandipb.net/2015/05/20/serializing-structured-data-into-avro-using-python/

        Args:
            avro_data (obj):            Avro data to write out.
            out_file_handle (str):      Open file handle to write avro
                                            data to.

        Returns:
            str - The path to the newly written file.
        """
        avro_writer = avro.datafile.DataFileWriter(
            out_file_handle,
            avro.io.DatumWriter(),
            self.schema,
            codec='deflate')

        avro_writer.append(avro_data)
        avro_writer.close()
