import sys

from rtl.processor import StreamProcessor as Processor
from rtl.transformer import Transformer


class EchoTransformer(Transformer):
    def map(self, message):
        return message.upper()

class KeyListingTransformer(Transformer):
    def map(self, message):
        return list(message.keys())


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'keys':
        # $ echo '{"test": 1, "a": 3, "c": 4}' | python3 example.py keys
        transformer = KeyListingTransformer()
    else:
        transformer = EchoTransformer()
        assert transformer.map('bla') == 'BLA'

    if len(sys.argv) > 1 and sys.argv[1] == 'keys':
        Processor('file:///dev/stdin', #'kafka://localhost/test?group.id=EchoTransformer',
                  transformer, 'file:///dev/stdout',
                  source_format='json', target_format='json').run()
    elif len(sys.argv) > 1 and sys.argv[1] == 'kafka':
        # $ python3 example.py kafka
        Processor('kafka://localhost/test?group.id=EchoTransformer2',
                  transformer, 'file:///dev/stdout').run()
    else:
        processor = Processor('file:///dev/stdin', transformer, 'file:///dev/stdout')
        processor.run()
