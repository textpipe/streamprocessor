import confluent_kafka

from base import Consumer, Producer



class KafkaConsumer(Consumer):
    def __init__(self, url, callback, **kwargs):
        super().__init__(url, callback, **kwargs)

        config = {'bootstrap.servers': self.parsed_url.netloc, 
                  'default.topic.config': {'auto.offset.reset': 'smallest'}}
        
        for k, v in self.parsed_url_qs.items():
            config[k] = v[0]

        self.kafka_consumer = confluent_kafka.Consumer(config)
        
    @property
    def kafka_topics(self):
        return [self.parsed_url.path[1:]]

    def consume(self):
        self.kafka_consumer.subscribe(self.kafka_topics)
        
        while True:
            msg = self.kafka_consumer.poll()
            if not msg.error():
                yield from self.callback(msg.value().decode('utf-8'))
            elif msg.error().code() != confluent_kafka.KafkaError._PARTITION_EOF:
                print(msg.error())
                break

        self.kafka_consumer.close()
