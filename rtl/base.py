from urllib.parse import urlparse, parse_qs


class URLParserMixin:
    def __init__(self, url):
        self.url = url

    @property
    def parsed_url(self):
        return urlparse(self.url)

    @property
    def parsed_url_qs(self):
        return parse_qs(self.parsed_url.query)


class Consumer(URLParserMixin):
    def __init__(self, url):
        URLParserMixin.__init__(self, url)

    def consume(self):
        raise NotImplementedError('Consumer is an abstract class.')


class Producer(URLParserMixin):
    def __init__(self, url, encode=lambda x: x):
        self.url = url
        self.encode = encode
    
    def produce(self, message):
        # Note that message should already be encoded
        raise NotImplementedError('Producer is an abstract class.')


class Transformer:
    def transform(self, message):
        yield from map(self.map, (message,))

    def map(self, message):
        raise NotImplementedError('Transformer is an abstract class.')


class StreamTransformer:
    def __init__(self, consumer, producer):
        self.consumer = consumer
        self.producer = producer

    def run(self):
        for message in self.consumer.consume():
            self.producer.produce(self.producer.encode(message))
