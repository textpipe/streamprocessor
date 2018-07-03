"""
Define a consumer that receives an input and can yield messages from it.
Define a producer that can produce the yielded message elsewhere.
"""
from urllib.parse import urlparse, parse_qs, parse_qsl
import logging


class ParsedURL:
    """
    Create a parsed URL instance of an URL which contains
    the scheme, netloc, path, params, query, fragment and
    the parsed query
    """
    def __init__(self, url):
        self.url = url

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url
        parsed_url = urlparse(self.url)
        self.scheme = parsed_url.scheme
        self.netloc = parsed_url.netloc
        self.path = parsed_url.path
        self.params = parsed_url.params
        self.query = parsed_url.query
        self.fragment = parsed_url.fragment

        self.qs = dict(parse_qsl(self.query))
        self.qs_dict = parse_qs(self.query)


class Consumer:
    """
    Select and create a consumer based on a URL
    """
    def __init__(self, url):
        self.url = ParsedURL(url)

    def consume(self):
        raise NotImplementedError('Consumer is an abstract class.')

    @staticmethod
    def create(url, *args, **kwargs):
        parsed_url = ParsedURL(url)
        if url == 'file:///dev/stdin':
            import streamprocessor.client.stdio
            return streamprocessor.client.stdio.StandardIOConsumer(url, *args, **kwargs)
        elif parsed_url.scheme.lower() == 'kafka':
            try:
                import streamprocessor.client.confluentkafka
            except ImportError:
                logging.warning('Cannot import Confluent Kafka during Consumer create, falling back to python-kafka.')
                try:
                    import streamprocessor.client.kafka
                except ImportError:
                    logging.error('Error importing Kafka during Consumer create.')
                    raise
                return streamprocessor.client.kafka.KafkaConsumer(url, *args, **kwargs)
            return streamprocessor.client.confluentkafka.ConfluentKafkaConsumer(url, *args, **kwargs)
        if parsed_url.scheme == 'file':
            import streamprocessor.client.file
            return streamprocessor.client.file.FileConsumer(url, *args, **kwargs)
        if parsed_url.scheme == 'elasticsearch':
            import streamprocessor.client.elasticsearch
            return streamprocessor.client.elasticsearch.ElasticsearchConsumer(url, *args, *kwargs)
        else:
            raise NotImplementedError(
                'Scheme {} is not supported'.format(parsed_url.scheme))


class Producer:
    """
    Select and create a producer based on a URL
    """
    def __init__(self, url):
        self.url = ParsedURL(url)

    def produce(self, message):
        # Note that message should already be encoded
        raise NotImplementedError('Producer is an abstract class.')

    @staticmethod
    def create(url, *args, **kwargs):
        parsed_url = ParsedURL(url)
        if url == 'file:///dev/stdout':
            import streamprocessor.client.stdio
            return streamprocessor.client.stdio.StandardIOProducer(url, *args, **kwargs)
        elif parsed_url.scheme == 'elasticsearch':
            import streamprocessor.client.elasticsearch
            return streamprocessor.client.elasticsearch.ElasticsearchProducer(url, *args, *kwargs)
        else:
            raise NotImplementedError(
                'Scheme {} is not supported'.format(parsed_url.scheme))
