from .. import exceptions
from .. import Server
from ..test import CouchTestCase
from ..utils import apply_schema_migration


class MigrateTest(CouchTestCase):
    def test_create_db(self):
        server = Server(alias='default')
        # Delete databases
        server.delete_database_if_exists('db1')
        server.delete_database_if_exists('db2')
        # Schema
        schema = dict(default=dict(db1=dict(), db2=dict()))
        apply_schema_migration(schema)
        databases = server.list_databases()
        self.assertIn('db1', databases)
        self.assertIn('db2', databases)


class MigrateDesignTest(CouchTestCase):
    def setUp(self):
        self.schema = dict(
            default=dict(
                db=dict(
                    designs=dict(
                        ddoc=dict(views=dict(view=dict())),
                    ),
                ),
            ),
        )

    def test_create_design(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        # Schema
        view = dict(map='function (doc) {\n  emit(doc._id, 1);\n}')
        self.schema['default']['db']['designs']['ddoc']['views']['view'] = view
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        data = db.get('_design/ddoc')
        self.assertEqual(data['_id'], '_design/ddoc')
        self.assertNotEqual(data['_rev'], None)
        self.assertEqual(data['views'], {'view': {'map': 'function (doc) {\n  emit(doc._id, 1);\n}'}})

    def test_create_design_compilation_error(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        # Schema
        view = dict(map='// Invalid function')
        self.schema['default']['db']['designs']['ddoc']['views']['view'] = view
        with self.assertRaises(exceptions.CouchError) as context:
            apply_schema_migration(self.schema)
        error = context.exception.args[0]
        self.assertEqual(error['error'], 'compilation_error')
        self.assertEqual(error['status_code'], 400)
        self.assertEqual(error['reason'], "Compilation of the map function in the 'view' view failed: Expression does not eval to a function. (// Invalid function)")

    def test_create_same_design(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        # Schema
        view = dict(map='function (doc) {\n  emit(doc._id, 1);\n}')
        self.schema['default']['db']['designs']['ddoc']['views']['view'] = view
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        data = db.get('_design/ddoc')
        self.assertEqual(data['_id'], '_design/ddoc')
        self.assertNotEqual(data['_rev'], None)
        self.assertEqual(data['views'], {'view': {'map': 'function (doc) {\n  emit(doc._id, 1);\n}'}})
        previous_rev = data['_rev']
        # Add same schema again
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        data = db.get('_design/ddoc')
        self.assertEqual(data['_id'], '_design/ddoc')
        self.assertEqual(data['_rev'], previous_rev)
        self.assertEqual(data['views'], {'view': {'map': 'function (doc) {\n  emit(doc._id, 1);\n}'}})

    def test_remove_design(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        view = dict(map='function (doc) {\n  emit(doc._id, 1);\n}')
        # Add 2 design doc
        self.schema['default']['db']['designs'] = dict(
            ddoc1=dict(views=dict(view=view)),
            ddoc2=dict(views=dict(view=view)),
        )
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        db.get('_design/ddoc1')
        db.get('_design/ddoc2')
        # ddoc1 is no more present in schema
        self.schema['default']['db']['designs'] = dict(
            ddoc2=dict(views=dict(view=view)),
        )
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        with self.assertRaises(exceptions.CouchError) as context:
            db.get('_design/ddoc1')
        self.assertEqual(context.exception.args[0]['error'], 'not_found')
        db.get('_design/ddoc2')


class MigrateIndexTest(CouchTestCase):
    def setUp(self):
        self.schema = dict(
            default=dict(
                db=dict(
                    index=dict(
                        ddoc=dict(index=dict(fields=['document_type'])),
                    ),
                ),
            ),
        )

    def test_create_index(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        # Schema
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        indexes = db.get('_index')['indexes']
        index = list(filter(lambda i: i['ddoc'] == '_design/ddoc', indexes))[0]
        self.assertEqual(index['ddoc'], '_design/ddoc')
        self.assertEqual(index['name'], 'index')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))

    def test_create_index_invalid(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        # Schema
        self.schema['default']['db']['index']['ddoc']['index'] = dict(wrong=['document_type'])
        with self.assertRaises(exceptions.CouchError) as context:
            apply_schema_migration(self.schema)
        error = context.exception.args[0]
        self.assertEqual(error['error'], 'missing_required_key')
        self.assertEqual(error['status_code'], 400)
        self.assertEqual(error['reason'], 'Missing required key: fields')

    def test_create_same_index(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        # Schema
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        indexes = db.get('_index')['indexes']
        index = list(filter(lambda i: i['ddoc'] == '_design/ddoc', indexes))[0]
        self.assertEqual(index['ddoc'], '_design/ddoc')
        self.assertEqual(index['name'], 'index')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))
        # Add same schema again
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        indexes = db.get('_index')['indexes']
        index = list(filter(lambda i: i['ddoc'] == '_design/ddoc', indexes))[0]
        self.assertEqual(index['ddoc'], '_design/ddoc')
        self.assertEqual(index['name'], 'index')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))

    def test_remove_index_1(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        # Add 2 design doc
        self.schema['default']['db']['index'] = dict(
            ddoc1=dict(
                index1=dict(fields=['document_type']),
                index2=dict(fields=['document_type']),
            ),
        )
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        self.assertTrue(db.get_index('ddoc1', 'index1'))
        self.assertTrue(db.get_index('ddoc1', 'index2'))
        # ddoc1 is no more present in schema
        self.schema['default']['db']['index'] = dict(
            ddoc1=dict(
                index2=dict(fields=['document_type']),
            ),
        )
        apply_schema_migration(self.schema)
        self.assertFalse(db.get_index('ddoc1', 'index1'))
        self.assertTrue(db.get_index('ddoc1', 'index2'))

    def test_remove_index_2(self):
        server = Server(alias='default')
        server.delete_database_if_exists('db')
        # Add 2 design doc
        self.schema['default']['db']['index'] = dict(
            ddoc1=dict(index1=dict(fields=['document_type'])),
            ddoc2=dict(index2=dict(fields=['document_type'])),
        )
        apply_schema_migration(self.schema)
        db = server.get_database('db')
        self.assertTrue(db.get_index('ddoc1', 'index1'))
        self.assertTrue(db.get_index('ddoc2', 'index2'))
        # ddoc1 is no more present in schema
        self.schema['default']['db']['index'] = dict(
            ddoc2=dict(index2=dict(fields=['document_type'])),
        )
        apply_schema_migration(self.schema)
        self.assertFalse(db.get_index('ddoc1', 'index1'))
        self.assertTrue(db.get_index('ddoc2', 'index2'))
