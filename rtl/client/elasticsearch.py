import elasticsearch
from client.base import Consumer, Producer

class ElasticsearchConsumer(Consumer):
    def __init__(self, url, **kwargs): 
        super().__init__(url, **kwargs)
        url_path_parts = self.url.path.split('/')
        assert len(url_path_parts) > 2
        self._es = elasticsearch.Elasticsearch(self.url.netloc)
        print(self.url.query)
        self.res = self._es.search(index=url_path_parts[1], body={"query": { url.query: {}}})


    def consume(self):
        for hit in self.res['hits']['hits']:
            yield hit


class ElasticsearchProducer(Producer):
    def __init__(self, url, **kwargs):
        super().__init__(url, **kwargs)
        url_path_parts = self.url.path.split('/')
        assert len(url_path_parts) > 2
        self._es_kwargs = {'index': url_path_parts[1],
                           'doc_type': url_path_parts[2]}
        self._es_kwargs.update({k: v for k, v in self.url.qs.items()
                                if k != 'id'})
        self.id_field = self.url.qs.get('id')
        self._es = elasticsearch.Elasticsearch(self.url.netloc)

    def produce(self, message):
        kwargs = dict(self._es_kwargs)
        if self.id_field:
            assert self.id_field in message
            kwargs['id'] = message[self.id_field]
        kwargs['body'] = message
        self._es.index(**kwargs)
