import itertools
import warnings
from django.test import override_settings
from django.test import TestCase
from couch.test import CouchTestCase
from .. import Database
from .. import documents
from .. import exceptions
from .. import Server


class Book(documents.Document):
    title = documents.TextField()
    pages = documents.IntegerField()

    class Meta:
        database_name = 'mydb'
        document_type = 'book'


class Author(documents.Document):
    name = documents.TextField()

    class Meta:
        database_name = 'mydb'
        document_type = 'author'


@override_settings(COUCH_SERVERS=dict(default=dict()))
class DatabaseNoCouchTest(TestCase):
    def test_init_default(self):
        db = Database('mydb')
        self.assertEqual(db.name, 'mydb')
        self.assertEqual(db.server.alias, 'default')
        self.assertEqual(db.server.protocol, 'http')
        self.assertEqual(db.server.host, 'localhost')
        self.assertEqual(db.server.port, 5984)
        self.assertEqual(db.server.username, None)
        self.assertEqual(db.server.password, None)

    @override_settings(COUCH_SERVERS=dict(default=dict(), another=dict(PORT=9999)))
    def test_init_by_name(self):
        db = Database('anotherdb', alias='another')
        self.assertEqual(db.name, 'anotherdb')
        self.assertEqual(db.server.alias, 'another')
        self.assertEqual(db.server.protocol, 'http')
        self.assertEqual(db.server.host, 'localhost')
        self.assertEqual(db.server.port, 9999)
        self.assertEqual(db.server.username, None)
        self.assertEqual(db.server.password, None)

    def test_init_by_instance(self):
        db = Database('customdb', server=Server(host='example.com'))
        self.assertEqual(db.name, 'customdb')
        self.assertEqual(db.server.alias, 'default')
        self.assertEqual(db.server.protocol, 'http')
        self.assertEqual(db.server.host, 'example.com')
        self.assertEqual(db.server.port, 5984)
        self.assertEqual(db.server.username, None)
        self.assertEqual(db.server.password, None)


class DatabaseTest(CouchTestCase):
    def setUp(self):
        self.server = Server()
        self.db1 = self.server.create_database('db1')
        self.db2 = self.server.create_database('db2')
        self.db1_name = self.db1._get_database_name()
        self.db2_name = self.db2._get_database_name()

    def test_database_name(self):
        self.assertEqual(self.db1._get_database_name(), 't_e_s_t__db1')
        self.assertEqual(self.db2._get_database_name(), 't_e_s_t__db2')

    def test_get(self):
        self.server.put('{}/docid1'.format(self.db1_name), json=dict())
        self.server.put('{}/docid2'.format(self.db2_name), json=dict())
        # db1
        data = self.db1.get('docid1')
        self.assertEqual(data['_id'], 'docid1')
        with self.assertRaises(exceptions.CouchError) as context:
            self.db1.get('docid2')
        self.assertEqual(context.exception.args[0]['error'], 'not_found')
        # db2
        data = self.db2.get('docid2')
        self.assertEqual(data['_id'], 'docid2')
        with self.assertRaises(exceptions.CouchError) as context:
            self.db2.get('docid1')
        self.assertEqual(context.exception.args[0]['error'], 'not_found')

    def test_get_acceptable_status_codes_ok(self):
        self.server.put('{}/docid'.format(self.db1_name), json=dict())
        data = self.db1.get('docid', acceptable_status_codes=[200])
        self.assertEqual(data['_id'], 'docid')

    def test_get_acceptable_status_codes_ko(self):
        self.server.put('{}/docid'.format(self.db1_name), json=dict())
        with self.assertRaises(exceptions.CouchError):
            self.db1.get('docid', acceptable_status_codes=[202])

    def test_put(self):
        self.db1.put('docid1', json=dict())
        self.db2.put('docid2', json=dict())
        # db1
        data = self.server.get('{}/docid1'.format(self.db1_name))
        self.assertEqual(data['_id'], 'docid1')
        with self.assertRaises(exceptions.CouchError) as context:
            self.server.get('{}/docid2'.format(self.db1_name))
        self.assertEqual(context.exception.args[0]['error'], 'not_found')
        # db2
        data = self.server.get('{}/docid2'.format(self.db2_name))
        self.assertEqual(data['_id'], 'docid2')
        with self.assertRaises(exceptions.CouchError) as context:
            self.server.get('{}/docid1'.format(self.db2_name))
        self.assertEqual(context.exception.args[0]['error'], 'not_found')

    def test_put_acceptable_status_codes_ok(self):
        self.db1.put('docid', json=dict(), acceptable_status_codes=[201])
        data = self.server.get('{}/docid'.format(self.db1_name))
        self.assertEqual(data['_id'], 'docid')

    def test_put_acceptable_status_codes_ko(self):
        with self.assertRaises(exceptions.CouchError):
            self.db1.put('docid', json=dict(), acceptable_status_codes=[202])

    def test_delete(self):
        data1 = self.db1.put('docid1', json=dict())
        data2 = self.db2.put('docid2', json=dict())
        # db1
        delete = self.db1.delete('docid1?rev={}'.format(data1['rev']))
        self.assertEqual(delete['id'], data1['id'])
        self.assertNotEqual(delete['rev'], data1['rev'])
        with self.assertRaises(exceptions.CouchError) as context:
            self.server.get('{}/docid1'.format(self.db1_name))
        self.assertEqual(context.exception.args[0]['error'], 'not_found')
        with self.assertRaises(exceptions.CouchError) as context:
            self.server.get('{}/docid2'.format(self.db1_name))
        self.assertEqual(context.exception.args[0]['error'], 'not_found')
        # db2
        delete = self.db2.delete('docid2?rev={}'.format(data2['rev']))
        self.assertEqual(delete['id'], data2['id'])
        self.assertNotEqual(delete['rev'], data2['rev'])
        with self.assertRaises(exceptions.CouchError) as context:
            self.server.get('{}/docid1'.format(self.db2_name))
        self.assertEqual(context.exception.args[0]['error'], 'not_found')
        with self.assertRaises(exceptions.CouchError) as context:
            self.server.get('{}/docid2'.format(self.db2_name))
        self.assertEqual(context.exception.args[0]['error'], 'not_found')

    def test_delete_acceptable_status_codes_ok(self):
        data = self.db1.put('docid', json=dict())
        delete = self.db1.delete('docid?rev={}'.format(data['rev']), acceptable_status_codes=[200])
        self.assertEqual(delete['id'], data['id'])
        self.assertNotEqual(delete['rev'], data['rev'])
        with self.assertRaises(exceptions.CouchError) as context:
            self.server.get('{}/docid'.format(self.db1_name))
        self.assertEqual(context.exception.args[0]['error'], 'not_found')

    def test_delete_acceptable_status_codes_ko(self):
        data = self.db1.put('docid', json=dict())
        with self.assertRaises(exceptions.CouchError):
            self.db1.delete('docid?rev={}'.format(data['rev']), acceptable_status_codes=[202])


class DatabaseListDocumentsTest(CouchTestCase):
    def test_list_design_documents(self):
        db = Server().create_database('mydb')
        db.put('_design/docid', json=dict(views=dict(view=dict(map='function (doc) {\n  emit(doc._id, 1);\n}'))))
        docs = db.list_design_documents()
        self.assertEqual(docs['total_rows'], 1)
        self.assertEqual(docs['offset'], 0)
        row = docs['rows'][0]
        self.assertEqual(row['id'], '_design/docid')
        self.assertEqual(row['key'], '_design/docid')
        self.assertNotEqual(row['value']['rev'], '')


class DatabaseViewTest(CouchTestCase):
    def setUp(self):
        self.db = Server().get_or_create_database('mydb')
        Book(_id='python_cookbook', title='Python Cookbook', pages=806).save()
        Book(_id='django_guide', title='The Definitive Guide to Django', pages=536).save()
        Author(_id='alex', name='Alex Martelli').save()
        Author(_id='adrian', name='Adrian Holovaty').save()

    def test_raw_empy_emit(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(); }'))))
        result = self.db.raw_view('viewdocid', 'view')
        self.assertEqual(result['offset'], 0)
        self.assertEqual(result['total_rows'], 4)
        self.assertEqual(len(result['rows']), 4)
        self.assertEqual(result['rows'][0], dict(id='adrian', key=None, value=None))
        self.assertEqual(result['rows'][1], dict(id='alex', key=None, value=None))
        self.assertEqual(result['rows'][2], dict(id='django_guide', key=None, value=None))
        self.assertEqual(result['rows'][3], dict(id='python_cookbook', key=None, value=None))

    def test_raw_emit_key(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.raw_view('viewdocid', 'view')
        self.assertEqual(result['offset'], 0)
        self.assertEqual(result['total_rows'], 4)
        self.assertEqual(len(result['rows']), 4)
        self.assertEqual(result['rows'][0], dict(id='adrian', key='adrian', value=None))
        self.assertEqual(result['rows'][1], dict(id='alex', key='alex', value=None))
        self.assertEqual(result['rows'][2], dict(id='django_guide', key='django_guide', value=None))
        self.assertEqual(result['rows'][3], dict(id='python_cookbook', key='python_cookbook', value=None))

    def test_raw_emit_key_value(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id, 1); }'))))
        result = self.db.raw_view('viewdocid', 'view')
        self.assertEqual(result['offset'], 0)
        self.assertEqual(result['total_rows'], 4)
        self.assertEqual(len(result['rows']), 4)
        self.assertEqual(result['rows'][0], dict(id='adrian', key='adrian', value=1))
        self.assertEqual(result['rows'][1], dict(id='alex', key='alex', value=1))
        self.assertEqual(result['rows'][2], dict(id='django_guide', key='django_guide', value=1))
        self.assertEqual(result['rows'][3], dict(id='python_cookbook', key='python_cookbook', value=1))

    def test_raw_limit(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(); }'))))
        result = self.db.raw_view('viewdocid', 'view', limit=2)
        self.assertEqual(result['offset'], 0)
        self.assertEqual(result['total_rows'], 4)
        self.assertEqual(len(result['rows']), 2)
        self.assertEqual(result['rows'][0]['id'], 'adrian')
        self.assertEqual(result['rows'][1]['id'], 'alex')

    def test_raw_startkey_1(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.raw_view('viewdocid', 'view', startkey='a')
        self.assertEqual(result['offset'], 0)
        self.assertEqual(result['total_rows'], 4)

    def test_raw_startkey_2(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.raw_view('viewdocid', 'view', startkey='b')
        self.assertEqual(result['offset'], 2)
        self.assertEqual(result['total_rows'], 4)

    def test_raw_startkey_3(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.raw_view('viewdocid', 'view', startkey='z')
        self.assertEqual(result['offset'], 4)
        self.assertEqual(result['total_rows'], 4)

    def test_raw_startkey_docid(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.raw_view('viewdocid', 'view', startkey='django_guide', startkey_docid='django_guide')
        self.assertEqual(result['offset'], 2)
        self.assertEqual(result['total_rows'], 4)

    def test_batch_1(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.view('viewdocid', batch_size=1)
        # The result is a generator so we map the first results to a list
        result = list(itertools.islice(result, 5))
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]['id'], 'adrian')
        self.assertEqual(result[1]['id'], 'alex')
        self.assertEqual(result[2]['id'], 'django_guide')
        self.assertEqual(result[3]['id'], 'python_cookbook')

    def test_batch_2(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.view('viewdocid', batch_size=2)
        # The result is a generator so we map the first results to a list
        result = list(itertools.islice(result, 5))
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]['id'], 'adrian')
        self.assertEqual(result[1]['id'], 'alex')
        self.assertEqual(result[2]['id'], 'django_guide')
        self.assertEqual(result[3]['id'], 'python_cookbook')

    def test_batch_3(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.view('viewdocid', batch_size=10)
        # The result is a generator so we map the first results to a list
        result = list(itertools.islice(result, 5))
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]['id'], 'adrian')
        self.assertEqual(result[1]['id'], 'alex')
        self.assertEqual(result[2]['id'], 'django_guide')
        self.assertEqual(result[3]['id'], 'python_cookbook')

    def test_batch_limit(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        result = self.db.view('viewdocid', batch_size=2, limit=3)
        # The result is a generator so we map the first results to a list
        result = list(itertools.islice(result, 5))
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['id'], 'adrian')
        self.assertEqual(result[1]['id'], 'alex')
        self.assertEqual(result[2]['id'], 'django_guide')

    def test_batch_ko_limit(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        with self.assertRaises(ValueError) as context:
            list(self.db.view('viewdocid', limit=0))
        self.assertEqual(context.exception.args, ('limit must be greater than 0',))

    def test_batch_ko_batch_size(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { emit(doc._id); }'))))
        with self.assertRaises(ValueError) as context:
            list(self.db.view('viewdocid', batch_size=0))
        self.assertEqual(context.exception.args, ('batch_size must be greater than 0',))

    def test_document_class(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { if(doc.document_type && doc.document_type=="book") { emit(doc._id, doc); }}'))))
        result = self.db.view('viewdocid', document_class=Book)
        result = list(itertools.islice(result, 5))
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], Book)
        self.assertEqual(result[0]._id, 'django_guide')
        self.assertEqual(result[0].title, 'The Definitive Guide to Django')
        self.assertEqual(result[0].pages, 536)
        self.assertNotEqual(result[0]._rev, None)
        self.assertIsInstance(result[1], Book)
        self.assertEqual(result[1]._id, 'python_cookbook')
        self.assertEqual(result[1].title, 'Python Cookbook')
        self.assertEqual(result[1].pages, 806)
        self.assertNotEqual(result[1]._rev, None)

    def test_document_class_type_mismatch(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { if(doc.document_type && doc.document_type=="author") { emit(doc._id, doc); }}'))))
        with self.assertRaises(exceptions.CouchError) as context:
            list(self.db.view('viewdocid', document_class=Book))
        self.assertEqual(context.exception.args[0], "Type mismatch error: document_type 'book' expected, got 'author'")


class DatabaseFindTest(CouchTestCase):
    def setUp(self):
        self.db = Server().get_or_create_database('mydb')
        Book(_id='python_cookbook', title='Python Cookbook', pages=806).save()
        Book(_id='django_guide', title='The Definitive Guide to Django', pages=536).save()
        Author(_id='alex', name='Alex Martelli').save()
        Author(_id='adrian', name='Adrian Holovaty').save()

    def test_ok(self):
        result = self.db.find(selector=dict(pages={'$gt': 700}), warning=False)
        # The result is a generator so we map the first results to a list
        result = list(itertools.islice(result, 5))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 'python_cookbook')
        self.assertEqual(result[0]['document_type'], 'book')
        self.assertEqual(result[0]['title'], 'Python Cookbook')
        self.assertEqual(result[0]['pages'], 806)
        self.assertNotEqual(result[0]['_rev'], None)

    def test_batch(self):
        result = self.db.find(selector=dict(), batch_size=2, warning=False)
        # The result is a generator so we map the first results to a list
        result = list(itertools.islice(result, 5))
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]['_id'], 'adrian')
        self.assertEqual(result[1]['_id'], 'alex')
        self.assertEqual(result[2]['_id'], 'django_guide')
        self.assertEqual(result[3]['_id'], 'python_cookbook')

    def test_batch_ko_limit(self):
        with self.assertRaises(ValueError) as context:
            list(self.db.find(selector=dict(), limit=0))
        self.assertEqual(context.exception.args, ('limit must be greater than 0',))

    def test_batch_ko_batch_size(self):
        with self.assertRaises(ValueError) as context:
            list(self.db.find(selector=dict(), batch_size=0))
        self.assertEqual(context.exception.args, ('batch_size must be greater than 0',))

    def test_document_class(self):
        result = self.db.find(selector=dict(document_type='author'), document_class=Author, warning=False)
        # The result is a generator so we map the first results to a list
        result = list(itertools.islice(result, 5))
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], Author)
        self.assertEqual(result[0].document_type, 'author')
        self.assertEqual(result[0]._id, 'adrian')
        self.assertEqual(result[0].name, 'Adrian Holovaty')
        self.assertNotEqual(result[0]._rev, None)
        self.assertIsInstance(result[1], Author)
        self.assertEqual(result[1].document_type, 'author')
        self.assertEqual(result[1]._id, 'alex')
        self.assertEqual(result[1].name, 'Alex Martelli')
        self.assertNotEqual(result[1]._rev, None)

    def test_document_class_type_mismatch(self):
        result = self.db.find(selector=dict(document_type='author'), document_class=Book, warning=False)
        with self.assertRaises(exceptions.CouchError) as context:
            list(itertools.islice(result, 5))
        self.assertEqual(context.exception.args[0], "Type mismatch error: document_type 'book' expected, got 'author'")

    def test_index_warning_true(self):
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            list(self.db.find(selector=dict(document_type='book'), warning=True, batch_size=1))
            self.assertEqual(len(warning_list), 1)
            message = warning_list[0].message.args[0]
            self.assertIn('no matching index found, create an index to optimize query time', message)
            self.assertIn("'selector': {'document_type': 'book'}", message)

    def test_index_warning_false(self):
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            list(self.db.find(selector=dict(document_type='book'), warning=False, batch_size=1))
            self.assertEqual(len(warning_list), 0)


class DatabaseIndexTest(CouchTestCase):
    def setUp(self):
        self.db = Server().get_or_create_database('mydb')

    def test_normalize_index(self):
        index = self.db.normalize_index(dict(fields=['document_type']))
        self.assertEqual(index,  {'fields': [{'document_type': 'asc'}]})

    def test_list_indexes(self):
        # Create index
        self.db.post('_index', json=dict(ddoc='ddoc1', name='index1', index=dict(fields=['document_type'])))
        # List indexes
        indexes = self.db.list_indexes()
        self.assertEqual(len(indexes), 2)
        # _all_docs
        index = indexes[(None, '_all_docs')]
        self.assertEqual(index['type'], 'special')
        self.assertEqual(index['ddoc'], None)
        self.assertEqual(index['name'], '_all_docs')
        self.assertEqual(index['def'], dict(fields=[dict(_id='asc')]))
        # index1
        index = indexes[('ddoc1', 'index1')]
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['ddoc'], 'ddoc1')
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))

    def test_list_indexes_filter_ddoc(self):
        # Create index
        self.db.post('_index', json=dict(ddoc='ddoc1', name='index1', index=dict(fields=['document_type'])))
        # List indexes
        indexes = self.db.list_indexes(ddoc='ddoc1')
        self.assertEqual(len(indexes), 1)
        # index1
        index = indexes[('ddoc1', 'index1')]
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['ddoc'], 'ddoc1')
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))

    def test_list_indexes_filter_name(self):
        # Create index
        self.db.post('_index', json=dict(ddoc='ddoc1', name='index1', index=dict(fields=['document_type'])))
        # List indexes
        indexes = self.db.list_indexes(name='_all_docs')
        self.assertEqual(len(indexes), 1)
        # _all_docs
        index = indexes[(None, '_all_docs')]
        self.assertEqual(index['type'], 'special')
        self.assertEqual(index['ddoc'], None)
        self.assertEqual(index['name'], '_all_docs')
        self.assertEqual(index['def'], dict(fields=[dict(_id='asc')]))

    def test_list_indexes_filter_ddoc_name(self):
        # Create index
        self.db.post('_index', json=dict(ddoc='ddoc1', name='index1', index=dict(fields=['document_type'])))
        # List indexes
        indexes = self.db.list_indexes(ddoc='ddoc1', name='index1')
        self.assertEqual(len(indexes), 1)
        # index1
        index = indexes[('ddoc1', 'index1')]
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['ddoc'], 'ddoc1')
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))

    def test_list_indexes_filter_not_found(self):
        # Create index
        self.db.post('_index', json=dict(ddoc='ddoc1', name='index1', index=dict(fields=['document_type'])))
        # List indexes
        indexes = self.db.list_indexes(ddoc='wrong', name='index1')
        self.assertEqual(len(indexes), 0)

    def test_get_index(self):
        self.db.post('_index', json=dict(ddoc='ddoc1', name='index1', index=dict(fields=['document_type'])))
        index = self.db.get_index(ddoc='ddoc1', name='index1')
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['ddoc'], 'ddoc1')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))

    def test_get_index_not_found(self):
        self.db.post('_index', json=dict(ddoc='ddoc1', name='index1', index=dict(fields=['document_type'])))
        index = self.db.get_index(ddoc='wrong', name='index1')
        self.assertEqual(index, None)

    def test_create_index(self):
        data = self.db.create_index(ddoc='ddoc1', name='index1', index=dict(fields=['document_type']))
        self.assertEqual(data['result'], 'created')
        self.assertEqual(data['ddoc'], 'ddoc1')
        self.assertEqual(data['name'], 'index1')
        # check index
        indexes = self.db.get('_index')['indexes']
        index = list(filter(lambda i: i['ddoc'] == '_design/ddoc1', indexes))[0]
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['ddoc'], '_design/ddoc1')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))

    def test_create_index_changed(self):
        data = self.db.create_index(ddoc='ddoc1', name='index1', index=dict(fields=['document_type']))
        self.assertEqual(data['result'], 'created')
        self.assertEqual(data['ddoc'], 'ddoc1')
        self.assertEqual(data['name'], 'index1')
        # check index
        indexes = self.db.get('_index')['indexes']
        index = list(filter(lambda i: i['ddoc'] == '_design/ddoc1', indexes))[0]
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['ddoc'], '_design/ddoc1')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))
        # Change index
        data = self.db.create_index(ddoc='ddoc1', name='index1', index=dict(fields=['_id']))
        self.assertEqual(data['result'], 'created')
        self.assertEqual(data['ddoc'], 'ddoc1')
        self.assertEqual(data['name'], 'index1')
        # check index
        indexes = self.db.get('_index')['indexes']
        index = list(filter(lambda i: i['ddoc'] == '_design/ddoc1', indexes))[0]
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['ddoc'], '_design/ddoc1')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(_id='asc')]))

    def test_create_index_unchanged(self):
        data = self.db.create_index(ddoc='ddoc1', name='index1', index=dict(fields=['document_type']))
        self.assertEqual(data['result'], 'created')
        self.assertEqual(data['ddoc'], 'ddoc1')
        self.assertEqual(data['name'], 'index1')
        # check index
        indexes = self.db.get('_index')['indexes']
        index = list(filter(lambda i: i['ddoc'] == '_design/ddoc1', indexes))[0]
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['ddoc'], '_design/ddoc1')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))
        # Same index
        data = self.db.create_index(ddoc='ddoc1', name='index1', index=dict(fields=dict(document_type='asc')))
        self.assertEqual(data['result'], 'unchanged')
        self.assertEqual(data['ddoc'], 'ddoc1')
        self.assertEqual(data['name'], 'index1')
        # check index
        indexes = self.db.get('_index')['indexes']
        index = list(filter(lambda i: i['ddoc'] == '_design/ddoc1', indexes))[0]
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['ddoc'], '_design/ddoc1')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))

    def test_delete_index(self):
        self.db.create_index(ddoc='ddoc1', name='index1', index=dict(fields=['document_type']))
        index = self.db.get_index(ddoc='ddoc1', name='index1')
        self.assertEqual(index['ddoc'], 'ddoc1')
        self.assertEqual(index['name'], 'index1')
        # Delete
        result = self.db.delete_index('ddoc1', 'index1')
        self.assertEqual(result['ok'], True)
        index = self.db.get_index(ddoc='ddoc1', name='index1')
        self.assertEqual(index, None)
