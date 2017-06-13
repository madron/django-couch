from .. import Server
from ..test import CouchTestCase
from ..documents import DesignDocument


class DesignDocumentTest(CouchTestCase):
    def setUp(self):
        self.db = Server(alias='default').get_or_create_database('db')

    def test_save_design(self):
        # Schema
        doc = DesignDocument(
            _id='_design/ddoc',
            views=dict(view=dict(map='function (doc) {\n  emit(doc._id, 1);\n}'))
        )
        self.assertEqual(doc._rev, None)
        doc._meta.database = self.db
        doc.save()
        self.assertNotEqual(doc._rev, None)

    def test_save_design_no_change(self):
        # Schema
        doc = DesignDocument(
            _id='_design/ddoc',
            views=dict(view=dict(map='function (doc) {\n  emit(doc._id, 1);\n}'))
        )
        doc._meta.database = self.db
        doc.save()
        previous_rev = doc._rev
        doc.save()
        self.assertEqual(doc._rev, previous_rev)

    def test_save_design_changed(self):
        # Schema
        doc = DesignDocument(
            _id='_design/ddoc',
            views=dict(view=dict(map='function (doc) {\n  emit(doc._id, 1);\n}'))
        )
        doc._meta.database = self.db
        doc.save()
        previous_rev = doc._rev
        doc.views = dict(view=dict(map='\nfunction (doc) {\n  emit(doc._id, 1);\n}'))
        doc.save()
        self.assertNotEqual(doc._rev, previous_rev)
