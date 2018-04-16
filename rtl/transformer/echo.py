from transformer.transformer import Transformer

class EchoTransformer(Transformer):
    def map(self, message):
        return message.upper()
