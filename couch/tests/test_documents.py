import datetime
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


class DocumentNoDbTest(CouchTestCase):
    def test_init_no_meta(self):
        class Book(documents.Document):
            pass

        document = Book()
        self.assertIsInstance(document, Book)
        self.assertEqual(document._meta.server_alias, 'default')
        self.assertEqual(document._meta.database_name, None)
        self.assertEqual(document._meta.database, None)
        self.assertEqual(document._meta.document_type, None)
        self.assertEqual(document._fields, dict())

    def test_init_no_database_name(self):
        class Book(documents.Document):
            class Meta:
                server_alias = 'another'

        document = Book()
        self.assertIsInstance(document, Book)
        self.assertEqual(document._meta.server_alias, 'another')
        self.assertEqual(document._meta.database_name, None)
        self.assertEqual(document._meta.database, None)
        self.assertEqual(document._meta.document_type, None)
        self.assertEqual(document._fields, dict())

    def test_init_no_server_alias(self):
        class Book(documents.Document):
            class Meta:
                database_name = 'db'

        document = Book()
        self.assertIsInstance(document, Book)
        self.assertEqual(document._meta.server_alias, 'default')
        self.assertEqual(document._meta.database_name, 'db')
        self.assertEqual(document._meta.database, None)
        self.assertEqual(document._meta.document_type, None)
        self.assertEqual(document._fields, dict())

    def test_init_document_type_none(self):
        class Book(documents.Document):
            class Meta:
                document_type = None

        document = Book()
        self.assertIsInstance(document, Book)
        self.assertIsInstance(document, dict)
        self.assertEqual(document._meta.server_alias, 'default')
        self.assertEqual(document._meta.database_name, None)
        self.assertEqual(document._meta.database, None)
        self.assertEqual(document._meta.document_type, None)
        self.assertEqual(document._fields, dict())

    def test_init(self):
        class Book(documents.Document):
            class Meta:
                server_alias = 'default'
                database_name = 'db'
                document_type = 'book'

        document = Book()
        self.assertIsInstance(document, Book)
        self.assertIsInstance(document, dict)
        self.assertEqual(document._meta.server_alias, 'default')
        self.assertEqual(document._meta.database_name, 'db')
        self.assertEqual(document._meta.database, None)
        self.assertEqual(document._meta.document_type, 'book')
        self.assertEqual(document._fields, dict())
        self.assertEqual(document.document_type, 'book')

    def test_init_fields(self):
        class Book(documents.Document):
            title = documents.TextField()
            pages = documents.IntegerField()
            published = documents.BooleanField(default=True)

        document = Book()
        self.assertEqual(document.title, None)
        self.assertEqual(document.pages, None)
        self.assertEqual(document.published, True)
        # _fields
        self.assertIsInstance(document._fields['title'], documents.TextField)
        self.assertIsInstance(document._fields['pages'], documents.IntegerField)
        self.assertIsInstance(document._fields['published'], documents.BooleanField)
        self.assertEqual(document._fields['title'].default, None)
        self.assertEqual(document._fields['pages'].default, None)
        self.assertEqual(document._fields['published'].default, True)

    def test_init_undefined_fields(self):
        class Book(documents.Document):
            title = documents.TextField()
            pages = documents.IntegerField()
            published = documents.BooleanField(default=True)

        document = Book(colour='red', age=12)
        self.assertEqual(document.title, None)
        self.assertEqual(document.pages, None)
        self.assertEqual(document.published, True)
        self.assertEqual(document.colour, 'red')
        self.assertEqual(document.age, 12)
        # _fields
        self.assertIsInstance(document._fields['title'], documents.TextField)
        self.assertIsInstance(document._fields['pages'], documents.IntegerField)
        self.assertIsInstance(document._fields['published'], documents.BooleanField)
        self.assertEqual(document._fields['title'].default, None)
        self.assertEqual(document._fields['pages'].default, None)
        self.assertEqual(document._fields['published'].default, True)

    def test_init_wrong_document_type(self):
        class Book(documents.Document):
            title = documents.TextField()

            class Meta:
                document_type = 'book'

        with self.assertRaises(exceptions.CouchError) as context:
            Book(document_type='author')
        self.assertEqual(context.exception.args[0], "Type mismatch error: document_type 'book' expected, got 'author'")

    def test_get_database(self):
        class Book(documents.Document):
            class Meta:
                database_name = 'db'

        Server().get_or_create_database('db')
        document = Book()
        self.assertEqual(document._meta.database, None)
        db1 = document._meta.get_database()
        self.assertEqual(document._meta.database, db1)
        db2 = document._meta.get_database()
        self.assertEqual(document._meta.database, db1)
        self.assertEqual(db1, db2)

    def test_attribute_init_empty(self):
        document = Book()
        self.assertEqual(document.title, None)
        self.assertEqual(document.pages, None)

    def test_attribute_init(self):
        document = Book(title='Python cookbook', author='Alex Martelli')
        self.assertEqual(document.title, 'Python cookbook')
        self.assertEqual(document.author, 'Alex Martelli')

    def test_attribute_set(self):
        document = Book()
        document.title = 'Python cookbook'
        document.author = 'Alex Martelli'
        self.assertEqual(document.title, 'Python cookbook')
        self.assertEqual(document.author, 'Alex Martelli')

    def test_attribute_set_id(self):
        document = Book()
        document._id = 'python_cookbook'
        self.assertEqual(document._id, 'python_cookbook')

    def test_attribute_set_rev(self):
        document = Book()
        document._rev = '123-987'
        self.assertEqual(document._rev, '123-987')

    def test_str_1(self):
        document = Book()
        self.assertEqual(str(document), 'Book')

    def test_str_2(self):
        document = Book(_id='python_cookbook')
        self.assertEqual(str(document), 'Book python_cookbook')

    def test_repr_1(self):
        class Book(documents.Document):
            title = documents.TextField()
            pages = documents.IntegerField()

        document = Book()
        self.assertEqual(repr(document), '{}')

    def test_repr_2(self):
        document = Book()
        self.assertEqual(repr(document), "{'document_type': 'book'}")

    def test_repr_3(self):
        text = repr(Book(_id='python_cookbook'))
        self.assertIn("'document_type': 'book'", text)
        self.assertIn("'_id': 'python_cookbook'", text)

    def test_repr_4(self):
        text = repr(Book(_id='python_cookbook', title='Python cookbook', pages=806))
        self.assertIn("'document_type': 'book'", text)
        self.assertIn("'_id': 'python_cookbook'", text)
        self.assertIn("'title': 'Python cookbook'", text)
        self.assertIn("'pages': 806", text)

    def test_bool_false(self):
        self.assertFalse(Book())

    def test_bool_true(self):
        self.assertTrue(Book(_id='python_cookbook'))


@override_settings(TIME_ZONE='UTC')
class DocumentTest(CouchTestCase):
    def setUp(self):
        self.server = Server()
        self.server.get_or_create_database('db')

    def test_create_empty(self):
        document = Book()
        document.save()
        self.assertIsInstance(document._id, str)
        self.assertIsInstance(document._rev, str)
        self.assertTrue(document._id)
        self.assertTrue(document._rev)
        self.assertEqual(document.document_type, 'book')
        # check db
        url = '/{}/{}'.format(self.server._get_database_name('db'), document._id)
        data = self.server.get(url)
        self.assertEqual(data['document_type'], 'book')
        self.assertEqual(data['_id'], document._id)
        self.assertEqual(data['_rev'], document._rev)

    def test_create_no_document_type(self):
        class Book(documents.Document):
            class Meta:
                database_name = 'db'

        document = Book()
        document.save()
        self.assertIsInstance(document._id, str)
        self.assertIsInstance(document._rev, str)
        self.assertTrue(document._id)
        self.assertTrue(document._rev)
        with self.assertRaises(AttributeError):
            document.document_type
        # check db
        url = '/{}/{}'.format(self.server._get_database_name('db'), document._id)
        data = self.server.get(url)
        self.assertNotIn('document_type', data)
        self.assertEqual(data['_id'], document._id)
        self.assertEqual(data['_rev'], document._rev)

    def test_create_no_database_name(self):
        class Book(documents.Document):
            pass

        document = Book()
        with self.assertRaises(exceptions.CouchError):
            document.save()

    def test_create(self):
        document = Book(
            title='Python cookbook',
            pages=806,
            weight=0.45,
            date=datetime.date(2013, 5, 1),
            datetime=pytz.timezone('Europe/Athens').localize(datetime.datetime(2013, 5, 1, 12, 0)),
            price=Decimal('49.99'),
            published=True,
        )
        document.save()
        self.assertIsInstance(document._id, str)
        self.assertIsInstance(document._rev, str)
        self.assertTrue(document._id)
        self.assertTrue(document._rev)
        # self.assertEqual(document.document_type, 'book')
        # check db
        url = '/{}/{}'.format(self.server._get_database_name('db'), document._id)
        data = self.server.get(url)
        self.assertEqual(data['document_type'], 'book')
        self.assertEqual(data['_id'], document._id)
        self.assertEqual(data['_rev'], document._rev)
        self.assertEqual(data['title'], 'Python cookbook')
        self.assertEqual(data['pages'], 806)
        self.assertEqual(data['weight'], 0.45)
        self.assertEqual(data['date'], '2013-05-01')
        self.assertEqual(data['datetime'], '2013-05-01T09:00:00+00:00')
        self.assertEqual(data['price'], '49.99')
        self.assertEqual(data['published'], True)

    def test_create_with_id(self):
        document = Book(
            _id='python_cookbook',
            title='Python cookbook',
            pages=806,
            weight=0.45,
            date=datetime.date(2013, 5, 1),
            datetime=pytz.timezone('Europe/Athens').localize(datetime.datetime(2013, 5, 1, 12, 0)),
            price=Decimal('49.99'),
            published=True,
        )
        document.save()
        # check db
        url = '/{}/{}'.format(self.server._get_database_name('db'), document._id)
        data = self.server.get(url)
        self.assertEqual(data['document_type'], 'book')
        self.assertEqual(data['_id'], 'python_cookbook')
        self.assertEqual(data['_rev'], document._rev)
        self.assertEqual(data['title'], 'Python cookbook')
        self.assertEqual(data['pages'], 806)
        self.assertEqual(data['weight'], 0.45)
        self.assertEqual(data['date'], '2013-05-01')
        self.assertEqual(data['datetime'], '2013-05-01T09:00:00+00:00')
        self.assertEqual(data['price'], '49.99')
        self.assertEqual(data['published'], True)

    def test_create_title_none(self):
        document = Book(
            _id='python_cookbook',
            title=None,
            undefined_field=None,
        )
        document.save()
        # check db
        url = '/{}/{}'.format(self.server._get_database_name('db'), 'python_cookbook')
        data = self.server.get(url)
        self.assertEqual(data['document_type'], 'book')
        self.assertEqual(data['_id'], 'python_cookbook')
        self.assertEqual(data['_rev'], document._rev)
        self.assertEqual(data['title'], None)
        self.assertEqual(data['undefined_field'], None)

    def test_update(self):
        document = Book(
            title='Python cookbook',
            pages=806,
            weight=0.45,
            date=datetime.date(2013, 5, 1),
            datetime=pytz.timezone('Europe/Athens').localize(datetime.datetime(2013, 5, 1, 12, 0)),
            price=Decimal('49.99'),
            published=True,
        )
        document.save()
        first_document_id = document._id
        first_document_rev = document._rev
        document.title = 'The Definitive Guide to Django'
        document.pages = 536
        document.weight = 0.35
        document.date = datetime.date(2009, 10, 20)
        document.datetime = datetime.datetime(2009, 10, 20, 12, 0, tzinfo=pytz.utc)
        document.price = Decimal('44.99')
        document.published = False
        document.save()
        self.assertEqual(document._id, first_document_id)
        self.assertNotEqual(document._rev, first_document_rev)
        self.assertEqual(document.document_type, 'book')
        # check db
        url = '/{}/{}'.format(self.server._get_database_name('db'), document._id)
        data = self.server.get(url)
        self.assertEqual(data['document_type'], 'book')
        self.assertEqual(data['_id'], document._id)
        self.assertEqual(data['_rev'], document._rev)
        self.assertEqual(data['title'], 'The Definitive Guide to Django')
        self.assertEqual(data['pages'], 536)
        self.assertEqual(data['weight'], 0.35)
        self.assertEqual(data['date'], '2009-10-20')
        self.assertEqual(data['datetime'], '2009-10-20T12:00:00+00:00')
        self.assertEqual(data['price'], '44.99')
        self.assertEqual(data['published'], False)

    def test_update_revision_mismatch(self):
        document1 = Book(_id='python_cookbook', title='Python cookbook')
        document1.save()
        document2 = Book.objects.get('python_cookbook')
        document2.title = 'Python cookbook v2'
        document2.save()
        document1.title = 'Python cookbook v3'
        with self.assertRaises(exceptions.RevisionMismatch) as context:
            document1.save()
        self.assertEqual(context.exception.args[0], dict(reason='Document update conflict.', status_code=409, error='conflict'))

    def test_update_revision_mismatch_override(self):
        document1 = Book(_id='python_cookbook', title='Python cookbook')
        document1.save()
        document2 = Book.objects.get('python_cookbook')
        document2.title = 'Python cookbook v2'
        document2.save()
        document1.title = 'Python cookbook v3'
        document1.save(revision_mismatch_override=True)
        # check db
        url = '/{}/{}'.format(self.server._get_database_name('db'), 'python_cookbook')
        data = self.server.get(url)
        self.assertEqual(data['title'], 'Python cookbook v3')

    def test_save_only_if_changed(self):
        document = Book(
            _id='python_cookbook',
            title='Python cookbook',
            pages=806,
            weight=0.45,
            date=datetime.date(2013, 5, 1),
            datetime=pytz.timezone('Europe/Athens').localize(datetime.datetime(2013, 5, 1, 12, 0)),
            price=Decimal('49.99'),
            published=True,
        )
        document.save()
        first_document_rev = document._rev
        document.save(only_if_changed=True)
        self.assertEqual(document._rev, first_document_rev)

    def test_save_only_if_changed_not_existing(self):
        document = Book(
            _id='python_cookbook',
            title='Python cookbook',
            pages=806,
            weight=0.45,
            date=datetime.date(2013, 5, 1),
            datetime=pytz.timezone('Europe/Athens').localize(datetime.datetime(2013, 5, 1, 12, 0)),
            price=Decimal('49.99'),
            published=True,
        )
        self.assertEqual(document._rev, None)
        document.save(only_if_changed=True)
        self.assertNotEqual(document._rev, None)

    def test_save_only_if_changed_no_id(self):
        document = Book(
            title='Python cookbook',
            pages=806,
            weight=0.45,
            date=datetime.date(2013, 5, 1),
            datetime=pytz.timezone('Europe/Athens').localize(datetime.datetime(2013, 5, 1, 12, 0)),
            price=Decimal('49.99'),
            published=True,
        )
        self.assertEqual(document._id, None)
        self.assertEqual(document._rev, None)
        document.save(only_if_changed=True)
        self.assertNotEqual(document._id, None)
        self.assertNotEqual(document._rev, None)

    def test_save_only_if_changed_changed(self):
        document = Book(
            _id='python_cookbook',
            title='Python cookbook',
            pages=806,
            weight=0.45,
            date=datetime.date(2013, 5, 1),
            datetime=pytz.timezone('Europe/Athens').localize(datetime.datetime(2013, 5, 1, 12, 0)),
            price=Decimal('49.99'),
            published=True,
        )
        document.save()
        first_document_rev = document._rev
        document.title = 'Php cookbook'
        document.save(only_if_changed=True)
        self.assertNotEqual(document._rev, first_document_rev)

    def test_delete(self):
        document = Book(_id='python_cookbook', title='Python cookbook')
        document.save()
        document.delete()
        url = '{}/python_cookbook'.format(self.server._get_database_name('db'))
        with self.assertRaises(exceptions.CouchError) as context:
            self.server.get(url)
        self.assertEqual(context.exception.args[0], dict(status_code=404, error='not_found', reason='deleted'))
