import datetime
import itertools
import pytz
from decimal import Decimal
from django.test import override_settings
from ..test import CouchTestCase
from .. import documents
from .. import exceptions
from .. import Server


class Book(documents.Document):
    title = documents.TextField()
    pages = documents.IntegerField()
    weight = documents.FloatField()
    date = documents.DateField()
    datetime = documents.DateTimeField()
    price = documents.DecimalField()
    published = documents.BooleanField()

    class Meta:
        database_name = 'db'
        document_type = 'book'


class Author(documents.Document):
    name = documents.TextField()

    class Meta:
        database_name = 'db'
        document_type = 'author'


@override_settings(TIME_ZONE='UTC')
class ManagerGetTest(CouchTestCase):
    def setUp(self):
        self.server = Server()
        self.server.get_or_create_database('db')

    def test_get(self):
        # Write data
        data = dict(
            title='Python cookbook',
            pages=806,
            weight=0.45,
            date='2013-05-01',
            datetime='2013-05-01T12:00:00+00:00',
            price='49.99',
            published=True,
        )
        data = self.server.post(self.server._get_database_name('db'), json=data)
        document_id = data['id']
        document_rev = data['rev']
        # Read document
        document = Book.objects.get(document_id)
        self.assertEqual(document.document_type, 'book')
        self.assertEqual(document._id, document_id)
        self.assertEqual(document._rev, document_rev)
        self.assertEqual(document.title, 'Python cookbook')
        self.assertEqual(document.pages, 806)
        self.assertEqual(document.weight, 0.45)
        self.assertEqual(document.date, datetime.date(2013, 5, 1))
        self.assertEqual(document.datetime, pytz.utc.localize(datetime.datetime(2013, 5, 1, 12, 0)))
        self.assertEqual(document.price, Decimal('49.99'))
        self.assertEqual(document.published, True)

    def test_get_field_null(self):
        # Write data
        data = dict(
            _id='python_cookbook',
            title=None,
            undefined_field=None,
        )
        data = self.server.post(self.server._get_database_name('db'), json=data)
        document_rev = data['rev']
        # Read document
        document = Book.objects.get('python_cookbook')
        self.assertEqual(document.document_type, 'book')
        self.assertEqual(document._id, 'python_cookbook')
        self.assertEqual(document._rev, document_rev)
        self.assertEqual(document.title, None)
        self.assertEqual(document.undefined_field, None)

    def test_get_undefined_fields(self):
        # Write data
        data = dict(
            _id='python_cookbook',
            date='2013-05-01',
            undefine_date='2013-05-01',
            colour='red',
            age=26,
        )
        self.server.post(self.server._get_database_name('db'), json=data)
        # Read document
        document = Book.objects.get('python_cookbook')
        self.assertEqual(document.title, None)
        self.assertEqual(document.date, datetime.date(2013, 5, 1))
        self.assertEqual(document.undefine_date, '2013-05-01')
        self.assertEqual(document.colour, 'red')
        self.assertEqual(document.age, 26)
        # Write document
        document.title = 'Python cookbook'
        document.one_more_undefined_field = 'Undefined'
        document.save()
        # Read data
        url = '/{}/{}'.format(self.server._get_database_name('db'), 'python_cookbook')
        data = self.server.get(url)
        self.assertEqual(data['_id'], 'python_cookbook')
        self.assertEqual(data['date'], '2013-05-01')
        self.assertEqual(data['undefine_date'], '2013-05-01')
        self.assertEqual(data['colour'], 'red')
        self.assertEqual(data['age'], 26)
        self.assertEqual(data['title'], 'Python cookbook')
        self.assertEqual(data['one_more_undefined_field'], 'Undefined')
        # Read document
        document = Book.objects.get('python_cookbook')
        self.assertEqual(document.title, 'Python cookbook')
        self.assertEqual(document.date, datetime.date(2013, 5, 1))
        self.assertEqual(document.undefine_date, '2013-05-01')
        self.assertEqual(document.colour, 'red')
        self.assertEqual(document.age, 26)
        self.assertEqual(document.one_more_undefined_field, 'Undefined')

    def test_get_document_type_mismatch(self):
        # Write data
        data = dict(
            _id='python_cookbook',
            document_type='author',
        )
        self.server.post(self.server._get_database_name('db'), json=data)
        # Read document
        with self.assertRaises(exceptions.CouchError) as context:
            Book.objects.get('python_cookbook')
        self.assertEqual(context.exception.args[0], "Type mismatch error: document_type 'book' expected, got 'author'")


class ManagerViewTest(CouchTestCase):
    def setUp(self):
        self.db, created = Server().get_or_create_database('db')
        Book(_id='python_cookbook', title='Python Cookbook', pages=806).save()
        Book(_id='django_guide', title='The Definitive Guide to Django', pages=536).save()
        Author(_id='alex', name='Alex Martelli').save()
        Author(_id='adrian', name='Adrian Holovaty').save()

    def test_ok(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { if(doc.document_type && doc.document_type=="book") { emit(doc._id, doc); }}'))))
        result = Book.objects.view('viewdocid')
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

    def test_document_type_mismatch(self):
        self.db.put('_design/viewdocid', json=dict(views=dict(view=dict(map='function(doc) { if(doc.document_type && doc.document_type=="author") { emit(doc._id, doc); }}'))))
        with self.assertRaises(exceptions.CouchError) as context:
            list(Book.objects.view('viewdocid'))
        self.assertEqual(context.exception.args[0], "Type mismatch error: document_type 'book' expected, got 'author'")


class ManagerFindTest(CouchTestCase):
    def setUp(self):
        self.db = Server().get_or_create_database('db')
        Book(_id='python_cookbook', title='Python Cookbook', pages=806).save()
        Book(_id='django_guide', title='The Definitive Guide to Django', pages=536).save()
        Author(_id='alex', name='Alex Martelli').save()
        Author(_id='adrian', name='Adrian Holovaty').save()

    def test_ok(self):
        result = Author.objects.find(selector=dict(document_type='author'), warning=False)
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

    def test_document_type_mismatch(self):
        result = Book.objects.find(selector=dict(document_type='author'), warning=False)
        with self.assertRaises(exceptions.CouchError) as context:
            list(itertools.islice(result, 5))
        self.assertEqual(context.exception.args[0], "Type mismatch error: document_type 'book' expected, got 'author'")


class ManagerFindOneTest(CouchTestCase):
    def setUp(self):
        self.db = Server().get_or_create_database('db')
        Book(_id='python_cookbook', title='Python Cookbook', pages=806).save()
        Book(_id='django_guide', title='The Definitive Guide to Django', pages=536).save()
        Author(_id='alex', name='Alex Martelli').save()
        Author(_id='adrian', name='Adrian Holovaty').save()

    def test_ok(self):
        result = Author.objects.find_one(selector=dict(name='Alex Martelli'), warning=False)
        self.assertIsInstance(result, Author)
        self.assertEqual(result.document_type, 'author')
        self.assertEqual(result._id, 'alex')
        self.assertEqual(result.name, 'Alex Martelli')
        self.assertNotEqual(result._rev, None)

    def test_document_type_mismatch(self):
        with self.assertRaises(exceptions.CouchError) as context:
            Book.objects.find_one(selector=dict(name='Alex Martelli'), warning=False)
        self.assertEqual(context.exception.args[0], "Type mismatch error: document_type 'book' expected, got 'author'")

    def test_find_one_not_found(self):
        with self.assertRaises(Author.DoesNotExist):
            Author.objects.find_one(selector=dict(name='not found'), warning=False)

    def test_find_one_multiple_objects(self):
        with self.assertRaises(Author.MultipleObjectsReturned):
            Author.objects.find_one(selector=dict(document_type='author'), warning=False)
