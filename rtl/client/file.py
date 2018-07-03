from rtl.client.base import Consumer, Producer


class FileConsumer(Consumer):
    def consume(self):
        with open(self.url.netloc) as infile:
            yield from infile

class FileProducer(Producer):
    def produce(self, message):
        with open(self.url.netloc, 'w') as outfile:
            outfile.write(message)
