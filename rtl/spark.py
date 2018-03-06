import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming.kinesis import KinesisUtils
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *

from base import Processor, Transformer


class SparkProcessor(Processor):
    def __init__(self, transformer, url):
        # super().__init__(self, transformer)
        self._transformer = transformer
        self._brokertype, self._topic, self._host = SparkProcessor.parse_url(url)

        self._windowsize = 30

    @staticmethod
    def parse_url(url):
        return "kafka", "sample", "localhost"

    @staticmethod
    def get_stream(brokertype, ssc):
        if brokertype.lower() == "kafka":
            return KafkaUtils.createStream(ssc)
        elif brokertype.lower() == "kinesis":
            return KinesisUtils.createStream(ssc)
        else:
            raise Exception("unsupported brokertype")

    def go(self):
        spark = (SparkSession
            .builder
            .appName(__name__)
            .getOrCreate())  
        
        df = spark \
          .readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("subscribe", "sample") \
          .load()

        fn = udf(lambda s: self._transformer.map(s), StringType())

        df = df \
            .withColumn('fn', fn(df.value)) \
            .withColumnRenamed("value", "oldvalue") \
            .withColumnRenamed("fn", "value")

        ds = df \
          .writeStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("checkpointLocation", "/private/tmp") \
          .option("topic", "out") \
          .start()

        ds.awaitTermination()


class EchoTransformer(Transformer):
    def map(self, message):
        message = message.decode('utf-8')
        return message.upper()


if __name__ == '__main__':
    sp = SparkProcessor(transformer=EchoTransformer(), url="kafka://localhost/sample")
    sp.go()
