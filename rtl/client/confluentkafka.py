from rtl.client.base import Consumer
import confluent_kafka
import os
import sys

class ConfluentKafkaConsumer(Consumer):
    def __init__(self, url, **kwargs):
        super().__init__(url, **kwargs)
        config = {'bootstrap.servers': self.url.netloc,
                  'default.topic.config': {'auto.offset.reset': 'beginning'}}
        config.update(self.url.qs)
        self.kafka_consumer = confluent_kafka.Consumer(config)

    @property
    def kafka_topics(self):
        return [self.url.path[1:]]

    def consume(self):
        self.kafka_consumer.subscribe(self.kafka_topics)
        while True:
            msg = self.kafka_consumer.poll()
            if not msg.error():
                yield msg.value().decode('utf-8')
            elif msg.error().code() != confluent_kafka.KafkaError._PARTITION_EOF:
                print(msg.error())
