class Transformer:

    def transform(self, message):
        yield from map(self.map, (message,))

    def map(self, message):
        raise NotImplementedError('Transformer is an abstract class.')
