import json
import warnings
from copy import deepcopy
from . import exceptions
from .server import Server


class Database(object):
    def __init__(self, name, alias='default', server=None):
        self.name = name
        self.server = server or Server(alias=alias)

    def _get_database_name(self):
        return self.server._get_database_name(self.name)

    def get(self, url, **kwargs):
        if url:
            url = '/{}'.format(url)
        url = '/{}{}'.format(self._get_database_name(), url)
        return self.server.get(url, **kwargs)

    def post(self, url, **kwargs):
        if url:
            url = '/{}'.format(url)
        url = '/{}{}'.format(self._get_database_name(), url)
        return self.server.post(url, **kwargs)

    def put(self, url, **kwargs):
        if url:
            url = '/{}'.format(url)
        url = '/{}{}'.format(self._get_database_name(), url)
        return self.server.put(url, **kwargs)

    def delete(self, url, **kwargs):
        if url:
            url = '/{}'.format(url)
        url = '/{}{}'.format(self._get_database_name(), url)
        return self.server.delete(url, **kwargs)

    def list_design_documents(self):
        return self.get('_all_docs?startkey="_design"&endkey="_design0"')

    def raw_view(self, document_name, view_name, **kwargs):
        params = dict()
        for name, value in kwargs.items():
            if name in ('key', 'startkey', 'endkey', 'start_key', 'end_key'):
                value = json.dumps(value)
            params[name] = value
        url = '_design/{}/_view/{}'.format(document_name, view_name)
        return self.get(url, params=params)

    def view(self, document_name, view_name='view', batch_size=100, document_class=None, **options):
        # Check sane batch size.
        if batch_size < 1:
            raise ValueError('batch_size must be greater than 0')
        # Save caller's limit, it must be handled manually.
        limit = options.get('limit')
        if limit is not None and limit < 1:
            raise ValueError('limit must be greater than 0')
        # Batch loop
        while True:
            loop_limit = min(limit or batch_size, batch_size)
            # Get rows in batches, with one extra for start of next batch.
            options['limit'] = loop_limit + 1
            rows = self.raw_view(document_name, view_name, **options)['rows']
            # Yield rows from this batch.
            for row in rows[:loop_limit]:
                if document_class:
                    yield document_class(**row['value'])
                else:
                    yield row
            # Decrement limit counter.
            if limit is not None:
                limit -= min(len(rows), batch_size)
            # Check if there is nothing else to yield.
            if len(rows) <= batch_size or (limit is not None and limit == 0):
                break
            # Update options with start keys for next loop.
            options.update(startkey=rows[-1]['key'],
                           startkey_docid=rows[-1]['id'], skip=0)

    def view_one(self, document_name, key, view_name='view', document_class=None, **kwargs):
        kwargs['limit'] = 2
        kwargs['startkey'] = key
        kwargs['endkey'] = key
        result = list(self.view(document_name, view_name=view_name, document_class=document_class, **kwargs))
        if len(result) == 0:
            raise exceptions.ObjectDoesNotExist()
        if len(result) > 1:
            raise exceptions.MultipleObjectsReturned()
        return result[0]

    def find(self, batch_size=100, document_class=None, warning=True, **kwargs):
        kwargs['skip'] = kwargs.get('skip', 0)
        # Check sane batch size.
        if batch_size < 1:
            raise ValueError('batch_size must be greater than 0')
        # Save caller's limit, it must be handled manually.
        limit = kwargs.get('limit')
        if limit is not None and limit < 1:
            raise ValueError('limit must be greater than 0')
        # Batch loop
        while True:
            kwargs['limit'] = min(limit or batch_size, batch_size)
            result = self.post('_find', json=kwargs)
            if warning and 'warning' in result:
                msg = '{} - Query: {}'.format(result['warning'], kwargs)
                warnings.warn(msg)
                warning = False
            docs = result['docs']
            # Yield rows from this batch.
            for doc in docs:
                if document_class:
                    yield document_class(**doc)
                else:
                    yield doc
            # Decrement limit counter.
            if limit is not None:
                limit -= min(len(docs), batch_size)
            # Check if there is nothing else to yield.
            if len(docs) < batch_size or (limit is not None and limit == 0):
                break
            # Update skip parameter
            kwargs['skip'] += batch_size

    def find_one(self, document_class=None, warning=True, **kwargs):
        kwargs['skip'] = 0
        kwargs['limit'] = 2
        result = list(self.find(batch_size=2, document_class=document_class, warning=warning, **kwargs))
        if len(result) == 0:
            raise exceptions.ObjectDoesNotExist()
        if len(result) > 1:
            raise exceptions.MultipleObjectsReturned()
        return result[0]

    def normalize_index(self, index):
        if 'fields' in index:
            fields = []
            for field in index['fields']:
                if isinstance(field, str):
                    field = {field: 'asc'}
                fields.append(field)
            index = deepcopy(index)
            index['fields'] = fields
        return index

    def list_indexes(self, ddoc=None, name=None):
        indexes = dict()
        for index in self.get('_index')['indexes']:
            if index['ddoc']:
                index['ddoc'] = index['ddoc'].replace('_design/', '', 1)
            keep = True
            if ddoc and not ddoc == index['ddoc']:
                keep = False
            if name and not name == index['name']:
                keep = False
            if keep:
                indexes[(index['ddoc'], index['name'])] = index
        return indexes

    def get_index(self, ddoc=None, name=None):
        indexes = self.list_indexes(ddoc=ddoc, name=name)
        return indexes.get((ddoc, name))

    def create_index(self, ddoc, name, index):
        index = self.normalize_index(index)
        # check existing index
        existing = self.list_indexes(ddoc=ddoc, name=name).get((ddoc, name), None)
        if existing:
            if existing['def'] == index:
                return dict(result='unchanged', ddoc=ddoc, name=name)
        data = dict(index=index, ddoc=ddoc, name=name)
        result = self.post('_index', json=data)
        result['ddoc'] = result.pop('id').replace('_design/', '', 1)
        return result

    def delete_index(self, ddoc, name):
        url = '_index/{ddoc}/json/{name}'.format(ddoc=ddoc, name=name)
        return self.delete(url)
