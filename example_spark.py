from rtl.transformer import Transformer
from rtl.spark import SparkProcessor


class EchoTransformer(Transformer):
    def map(self, message):
        # TODO: Remove this from Transformer
        message = message.decode('utf-8')
        return message.upper()


if __name__ == '__main__':
    sp = SparkProcessor("kafka://localhost:9092/sample",
                        EchoTransformer(),
                        "kafka://localhost:9092/out")
    sp.run()
