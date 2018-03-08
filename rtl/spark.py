import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming.kinesis import KinesisUtils
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *

from base import Processor, Transformer, ParsedURL


class SparkProcessor(Processor):
    def __init__(self, source_url, transformer, target_url):
        self.transformer = transformer
        self.source_url = ParsedURL(source_url)
        self.target_url = ParsedURL(target_url)
        self._windowsize = 30

    @staticmethod
    def get_stream(brokertype, ssc):
        if brokertype.lower() == "kafka":
            return KafkaUtils.createStream(ssc)
        elif brokertype.lower() == "kinesis":
            return KinesisUtils.createStream(ssc)
        else:
            raise Exception("unsupported brokertype")

    def run(self):
        spark = (SparkSession.builder
            .appName(__name__)
            .getOrCreate())  
        
        # TODO: Kinesis
        df = spark \
          .readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", self.source_url.netloc) \
          .option("subscribe", self.source_url.path[1:]) \
          .load()

        fn = udf(lambda s: self.transformer.map(s), StringType())

        df = df.withColumn('value', fn(df.value))

        # TODO: Kinesis
        ds = df \
          .writeStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", self.target_url.netloc) \
          .option("checkpointLocation", "/private/tmp") \
          .option("topic", self.target_url.path[1:]) \
          .start()

        ds.awaitTermination()


class EchoTransformer(Transformer):
    def map(self, message):
        # TODO: Remove this from Transformer
        message = message.decode('utf-8')
        return message.upper()


if __name__ == '__main__':
    sp = SparkProcessor("kafka://localhost:9092/sample",
                        EchoTransformer(),
                        "kafka://localhost:9092/out")
    sp.run()
