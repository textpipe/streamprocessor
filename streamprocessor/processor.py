"""
Define a process for a document stream by creating a consumer and producer for 
the document and applying a transformer to it. 
"""

import pyspark
import tempfile
from pyspark import SparkConf, SparkContext
from streamprocessor.client.base import Consumer, Producer, ParsedURL
from pyspark.streaming.kinesis import KinesisUtils
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Processor:

    def run(self):
        raise NotImplementedError('Processor is an abstract class.')


class StreamProcessor(Processor):
    """
    Applies a transformer to a realtime feed.
    """
    def __init__(self, source_url, transformer, target_url,
                 source_format=None, target_format=None):
        """:param source_url: URL describing source data feed
        :type source_url: string
        :param transformer: Transformer
        :type transformer: :class Transformer
        :param target_url: URL describing destination data feed
        :type target_url: string
        :param source_format: desired source format
        :type: string
        :param target_format: desired target format
        :type: string
        """
        self.consumer = Consumer.create(source_url)
        self.producer = Producer.create(target_url)
        self.transformer = transformer

        self.setup_decoding(source_format)
        self.setup_encoding(target_format)

    def run(self):
        """
        Start the stream transformation
        """
        for message in self.consumer.consume():
            decoded = self.decode(message)
            for output in self.transformer.transform(decoded):
                self.producer.produce(self.encode(output))

    def setup_decoding(self, source_format):
        """
        Define decode functions for different source formats

        :param source_format: the format the input should be
        :type source_format: string
        """
        if source_format == 'json':
            import json
            self.decode = json.loads
        elif source_format is None:
            self.decode = lambda x: x
        else:
            raise NotImplementedError(
                'Source format {} is not supported'.format(source_format))

    def setup_encoding(self, target_format):
        """
        Define encode functions for different source formats

        :param target_format: the format the output should be
        :type target_format: string
        """
        if target_format == 'json':
            import json
            self.encode = json.dumps
        elif target_format is None:
            self.encode = lambda x: x
        else:
            raise NotImplementedError(
                'Target format {} is not supported'.format(target_format))


class SparkProcessor(Processor):
    """
    Applies a transformer to a realtime feed using
    Apache Spark as execution engine. Supports either
    Kafka or Kinesis.
    """
    def __init__(self, source_url, transformer, target_url):
        """:param source_url: URL describing source data feed
        :type source_url: string
        :param transformer: Transformer
        :type transformer: :class Transformer
        :param target_url: URL describing destination data feed
        :type target_url: string
        """
        self.transformer = transformer
        self.source_url = ParsedURL(source_url)
        self.target_url = ParsedURL(target_url)
        self._windowsize = 30  # Apache Spark microbatch streaming windowsize

    @staticmethod
    def format(url):
        """
        Returns brokertype as a string, in the format that Apache
        Spark expects it.

        :param url: URL from which to extract the brokertype
        :type url: string
        """
        return url.scheme.lower()

    def run(self):
        """
        Start the stream transformation
        """
        spark = (SparkSession.builder
                 .appName(__name__)
                 .getOrCreate())

        df = spark \
            .readStream \
            .format(SparkProcessor.format(self.source_url)) \
            .option("kafka.bootstrap.servers", self.source_url.netloc) \
            .option("subscribe", self.source_url.path[1:]) \
            .load()

        fn = udf(lambda s: self.transformer.map(s), StringType())

        df = df.withColumn('value', fn(df.value))
        with tempfile.TemporaryDirectory() as tmpdirname:
            ds = df \
                .writeStream \
                .format(SparkProcessor.format(self.target_url)) \
                .option("kafka.bootstrap.servers", self.target_url.netloc) \
                .option("checkpointLocation", tmpdirname) \
                .option("topic", self.target_url.path[1:]) \
                .start()

        ds.awaitTermination()
