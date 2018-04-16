from transformer.transformer import Transformer

class KeyListingTransformer(Transformer):
    def map(self, message):
        return list(message.keys())
