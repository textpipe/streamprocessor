"""
Define a process for a document stream by creating a consumer and producer for 
the document and applying a transformer to it. 
"""

from streamprocessor.client.base import Consumer, Producer


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
