import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.streaming.kinesis import KinesisUtils
from pyspark.streaming.kafka import KafkaUtils

class UnsupportedBrokerException(Exception):
	pass


class SparkKafkaMessageBroker(MessageBroker):
    @staticmethod
    def stream(source_url, transformer, target_url, 
               source_format=None, target_format=None):
        producer = MessageBroker.get_producer(target_url, target_format)
        return SparkStreamTransformer(source_url, transformer.map, producer)


class SparkStreamTransformer(StreamTransformer):
	# https://spark.apache.org/docs/2.2.0/streaming-kafka-0-10-integration.html
	def __init__(self, producer):		
		#foreachMessage, topic, windowsize=120, task):
		"""Constructor

		* instantiate spark
		* instantiate sparkContext
		* instantiate sparkStreamingContext

		Keyword Arguments:
			foreachMessage {function} -- logic to deal with messages
			windowsize {number} -- [description] (default: {120})
		"""
		self._topic = topic

		self._spark = (SparkSession
			.builder
			.appName(args.appname)
			.getOrCreate())  
		self._spark_conf = (SparkConf().setAppName("MessageBroker"))
		self._sc = SparkContext(conf = spark_conf)
		self._ssc = StreamingContext(self._sc, windowsize)
		self._stream = _get_stream()
		self._foreachMessage = foreachMessage
		self._udf = udf(self._foreachMessage) # wrap underlying function in Spark UDF

		# https://spark.apache.org/docs/2.2.0/sql-programming-guide.html
		self\
			.stream\
			.foreachMessage(lambda x: self._udf()(x)) # convert to dataframe first

	def run(self):
		self._ssc.start()
		self._ssc.awaitTermination()

	class KafkaBroker(MessageBroker):
		# http://kafka.apache.org/

		def _get_stream(self):
			return KafkaUtils.createStream(self._ssc)

		def push(self, message):
			pass


	class KinesisBroker(MessageBroker):

		def _get_stream(self):
			return KinesisUtils.createStream(self._ssc)

		def push(self, message):
			pass
