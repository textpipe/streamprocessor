from base import StreamTransformer

import stdio
import kafka
import json

from urllib.parse import urlparse


class UnsupportedBrokerException(Exception):
    pass


class MessageBroker:
    @staticmethod
    def stream(source_url, transformer, target_url, 
               source_format=None, target_format=None):
        consumer = MessageBroker.get_consumer(source_url, transformer.transform, source_format)
        producer = MessageBroker.get_producer(target_url, target_format)
        return StreamTransformer(consumer, producer)


    @staticmethod
    def get_consumer(url, callback, source_format, *args, **kwargs):
        parsed_url = urlparse(url)
        if url == 'file:///dev/stdin':
            consumer = stdio.StandardIOConsumer(url, callback, *args, **kwargs)
        elif parsed_url.scheme.lower() == 'kafka':
            consumer = kafka.KafkaConsumer(url, callback, *args, **kwargs)
        else:
            raise UnsupportedBrokerException()

        if source_format == 'json':
            consumer.decode = json.loads
        elif source_format is not None:
            raise NotImplemented('Source format {} is not supported'.format(source_format))
        
        return consumer

    @staticmethod
    def get_producer(url, target_format, *args, **kwargs):
        if url == 'file:///dev/stdout':
            producer = stdio.StandardIOProducer(url, *args, **kwargs)
        else:
            raise UnsupportedBrokerException()
        
        if target_format == 'json':
            producer.encode = json.dumps
        elif target_format is not None:
            raise NotImplemented('Target format {} is not supported'.format(target_format))

        return producer