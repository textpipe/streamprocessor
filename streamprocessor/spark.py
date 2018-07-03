import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming.kinesis import KinesisUtils
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *
import tempfile

from streamprocessor.client.base import ParsedURL
from streamprocessor.processor import Processor


class SparkProcessor(Processor):
    def __init__(self, source_url, transformer, target_url):
        """Applies the transformer to a realtime feed using 
        Apache Spark as execution engine. Supports either
        Kafka or Kinesis.
        
        [description]
        :param source_url: URL describing source data feed
        :type source_url: string
        :param transformer: Transformer
        :type transformer: :class Transformer
        :param target_url: URL describing destination data feed
        :type target_url: string
        """
        self.transformer = transformer
        self.source_url = ParsedURL(source_url)
        self.target_url = ParsedURL(target_url)
        self._windowsize = 30 # Apache Spark microbatch streaming windowsize

    @staticmethod
    def format(url):
        """Returns brokertype as a string, in the format that Apache 
        Spark expects it.
        
        :param url: URL from which to extract the brokertype
        :type url: string
        """
        return url.scheme.lower()

    def run(self):
        """Start the stream transformation
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
