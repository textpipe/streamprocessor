import sys

from base import Consumer, Producer


class StandardIOConsumer(Consumer):
    def consume(self):
        yield from sys.stdin


class StandardIOProducer(Producer):
    def produce(self, message):
        print(message)
