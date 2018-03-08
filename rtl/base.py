from urllib.parse import urlparse, parse_qs, parse_qsl


class ParsedURL:

    def __init__(self, url):
        self.url = url

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url
        parsed_url = urlparse(self.url)
        for field in parsed_url._fields:
            setattr(self, field, getattr(parsed_url, field))
        self.qs = dict(parse_qsl(self.query))
        self.qs_dict = parse_qs(self.query)


class Consumer:

    def __init__(self, url):
        self.url = ParsedURL(url)

    def consume(self):
        raise NotImplementedError('Consumer is an abstract class.')

    @staticmethod
    def create(url, *args, **kwargs):
        parsed_url = ParsedURL(url)
        if url == 'file:///dev/stdin':
            import stdio
            return stdio.StandardIOConsumer(url, *args, **kwargs)
        elif parsed_url.scheme.lower() == 'kafka':
            try:
                import kafka
            except ImportError:
                logging.error('Error importing Kafka during Consumer create.')
                raise
            return kafka.KafkaConsumer(url, *args, **kwargs)
        else:
            raise NotImplemented(
                'Scheme {} is not supported'.format(parsed_url.scheme))


class Producer:

    def __init__(self, url):
        self.url = ParsedURL(url)

    def produce(self, message):
        # Note that message should already be encoded
        raise NotImplementedError('Producer is an abstract class.')

    @staticmethod
    def create(url, *args, **kwargs):
        parsed_url = ParsedURL(url)
        if url == 'file:///dev/stdout':
            import stdio
            return stdio.StandardIOProducer(url, *args, **kwargs)
        else:
            raise NotImplemented(
                'Scheme {} is not supported'.format(parsed_url.scheme))


class Transformer:

    def transform(self, message):
        yield from map(self.map, (message,))

    def map(self, message):
        raise NotImplementedError('Transformer is an abstract class.')


class Processor:

    def run(self):
        raise NotImplementedError('Processor is an abstract class.')


class StreamProcessor(Processor):

    def __init__(self, source_url, transformer, target_url,
                 source_format=None, target_format=None):
        self.consumer = Consumer.create(source_url)
        self.producer = Producer.create(target_url)
        self.transformer = transformer

        self.setup_decoding(source_format)            
        self.setup_encoding(target_format)

    def run(self):
        for message in self.consumer.consume():
            decoded = self.decode(message)
            for output in self.transformer.transform(decoded):
                self.producer.produce(self.encode(output))

    def setup_decoding(self, source_format):
        if source_format == 'json':
            import json
            self.decode = json.loads
        elif source_format is None:
            self.decode = lambda x: x
        else:
            raise NotImplemented(
                'Source format {} is not supported'.format(source_format))

    def setup_encoding(self, target_format):
        if target_format == 'json':
            import json
            self.encode = json.dumps
        elif target_format is None:
            self.encode = lambda x: x
        else:
            raise NotImplemented(
                'Target format {} is not supported'.format(target_format))
