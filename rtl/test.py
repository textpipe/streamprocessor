import sys
from transformer.keylisting import KeyListingTransformer
from transformer.echo import EchoTransformer
from RNG.RTLnieuwstransformer import RTLNieuwsTransformer
from processor import StreamProcessor as Processor


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'keys':
        # $ echo '{"test": 1, "a": 3, "c": 4}' | python3 rtl/transformer.py keys
        transformer = KeyListingTransformer()
    elif len(sys.argv) > 1 and sys.argv[1] == 'elasticsearch':
        transformer = RTLNieuwsTransformer()
    else:
        transformer = EchoTransformer()
        assert transformer.map('bla') == 'BLA'

    if len(sys.argv) > 1 and sys.argv[1] == 'keys':
        Processor('file:///dev/stdin', #'kafka://localhost/test?group.id=EchoTransformer',
                  transformer, 'file:///dev/stdout',
                  source_format='json', target_format='json').run()
    elif len(sys.argv) > 1 and sys.argv[1] == 'kafka':
        # $ python3 rtl/transformer.py kafka
        Processor('kafka://localhost/test?group.id=EchoTransformer2',
                  transformer, 'file:///dev/stdout').run()
    elif len(sys.argv) > 1 and sys.argv[1] == 'elasticsearch':
        Processor('file://' + sys.argv[2],
                  transformer, 'elasticsearch://localhost:9200/rtlnieuws/article?id=nid',
                  source_format='json').run()
    else:
        processor = Processor('file:///dev/stdin', transformer, 'file:///dev/stdout')
        processor.run()
