from client.base import Consumer
import kafka
import os
import sys

def convert_value(v):
    try:
        return int(v)
    except:
        try:
            return float(v)
        except:
            return v


class KafkaConsumer(Consumer):
    def __init__(self, url, **kwargs):
        super().__init__(url, **kwargs)
        config = {
            'bootstrap_servers': self.url.netloc,
            'auto_offset_reset': os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        }
        config.update({k.replace('.', '_'): convert_value(v) for k, v in self.url.qs.items()})
        self.kafka_consumer = kafka.KafkaConsumer(self.url.path[1:], **config)

    def consume(self):
        for message in self.kafka_consumer:
            yield message.value.decode('utf-8')
