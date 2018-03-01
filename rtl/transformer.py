import sys

from messagebroker import MessageBroker
from base import Transformer


class EchoTransformer(Transformer):
    def map(self, message):
        return message.upper()


class KeyListingTransformer(Transformer):
    def map(self, message):
        return list(message.keys())


if __name__ == '__main__':
    transformer = EchoTransformer()
    assert transformer.map('bla') == 'BLA'

    if len(sys.argv) > 1 and sys.argv[1] == 'keys':
        # $ echo '{"test": 1, "a": 3, "c": 4}' | python3 rtl/transformer.py keys
        transformer = KeyListingTransformer()
        MessageBroker.stream('file:///dev/stdin', #'kafka://localhost/test?group.id=EchoTransformer', 
                            transformer, 'file:///dev/stdout',
                            source_format='json', target_format='json').run()
    elif len(sys.argv) > 1 and sys.argv[1] == 'kafka':
        # $ python3 rtl/transformer.py kafka
        MessageBroker.stream('kafka://localhost/test?group.id=EchoTransformer',
                            transformer, 'file:///dev/stdout').run()
    else:
        MessageBroker.stream('file:///dev/stdin', transformer, 'file:///dev/stdout').run()