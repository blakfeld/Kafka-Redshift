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

import io
import logging
import os
import tempfile
import uuid

import avro.protocol
import boto3
import psycopg2
from avro.datafile import DataFileWriter
from avro.io import DatumWriter, DatumReader, BinaryDecoder
from kafka import KafkaConsumer

import utils


class KafkaRedshift(object):
    """
    Kafka Consumer and Redshift copier.
    """

    def __init__(self,
                 kafka_topic,
                 kafka_hosts,
                 avro_protocol_path,
                 schema_name,
                 s3_bucket,
                 redshift_host,
                 redshift_dbname,
                 redshift_user,
                 redshift_pass,
                 redshift_port='5439'):
        """
        Constructor.

        kafka_topic (str):          The Kafka topic to consume.
        kafka_hosts (list):         List of Kafka nodes to connect
                                        to.
        avro_protocol_path (str):   Path to Avro Schema file.
        redshift_host (str):        Redshift host to copy data to.
        redshift_dbname (str):      Name of the Redshift database to
                                        use.
        redshift_user (str):        The user to connect to Redshift
                                        as.
        redshift_pass (str):        The password to use when
                                        connecting to Redshift.
        """
        # Really psycopg? You couldn't wrap this a little better?
        conn_string = """
            host='{0}' dbname='{1}' port={2} user='{3}' password='{4}'
            """.format(redshift_host,
                       redshift_dbname,
                       redshift_port,
                       redshift_user,
                       redshift_pass).strip()

        self.conn = psycopg2.connect(conn_string)
        self.consumer = KafkaConsumer(kafka_topic,
                                      bootstrap_servers=kafka_hosts)
        self.avro_protocol = avro.protocol.parse(
            utils.convert_avdl_to_avpr(avro_protocol_path))
        self.schema_name = schema_name
        self.schema = self.avro_protocol.types_dict[self.schema_name]
        self.s3_client = boto3.client('s3')
        self.s3_bucket = s3_bucket

    def run(self):
        """
        Main execution loop.
        """
        logging.debug('Running Main Loop...')
        for msg in self.consumer:
            decoded_msg = self._decode_avro(msg)
            logging.debug(decoded_msg)

            temp_file = tempfile.NamedTemporaryFile(delete=False)
            self._write_avro(decoded_msg, temp_file)

            s3_key = '{0}.{1}.avro'.format(self.schema_name, uuid.uuid4())
            self._upload_to_s3(temp_file.name, s3_key)

            os.unlink(temp_file.name)

            self._copy_to_redshift(s3_key)

    def _decode_avro(self, avro_data):
        """
        Decode Avro Message.

        Args:
            avro_data (binary):     Avro data to decode.

        Returns:
            dict
        """
        bytes_reader = io.BytesIO(avro_data.value)
        decoder = BinaryDecoder(bytes_reader)
        reader = DatumReader(self.schema)

        return reader.read(decoder)

    def _write_avro(self, avro_data, file_obj):
        """
        Compress avro data and write it to disk.

        NOTE:
            Using the "deflate" codec as opposed to something like
            "snappy" because "deflate" actually seems to produce
            smaller files. At least according to this dude:

            https://blog.sandipb.net/2015/05/20/serializing-structured-data-into-avro-using-python/

        Args:
            avro_data (obj):    Avro data to write out.
            file_obj(str):      Open file handle to write avro data
                                    to.

        Returns:
            str - The path to the newly written file.

        """
        with DataFileWriter(file_obj, DatumWriter(), self.schema,
                            codec='deflate') as avro_writer:
            avro_writer.append(avro_data)

    def _upload_to_s3(self, fpath, key):
        """
        Upload a file to s3.

        Args:
            fpath (str):    File path to upload.
            key (str):      Object Key name to use.
        """
        self.s3_client.upload_file(fpath, self.s3_bucket, key)

    def _copy_to_redshift(self, s3_key):
        """
        Copy data from a provided S3 Bucket to Redshift.

        Args:
            s3_key (str):   The object key you wish to copy to redshift.
        """
        sql = 'COPY data FROM "s3://{0}/{1}" FORMAT AS AVRO "auto"'.format(self.bucket, s3_key)
        self.conn.execute(sql)


