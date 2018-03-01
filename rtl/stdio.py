import sys

from base import Consumer, Producer


class StandardIOConsumer(Consumer):
    def consume(self):
        for line in sys.stdin:
            yield from self.callback(line)


class StandardIOProducer(Producer):
    def produce(self, message):
        print(message)