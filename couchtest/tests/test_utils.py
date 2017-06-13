from django.test import TestCase
from couch import Server
from couch.test import CouchTestCase
from couch.utils import collect_schema
from couch.utils import merge_schema
from couch.utils import migrate


class UtilsTest(TestCase):
    def test_collect_schema(self):
        schema = collect_schema()
        app = schema['couchtest']
        server = app['default']

        # ctdb
        db = server['ctdb']
        designs = db['designs']
        # ctdb testdesigndoc
        doc = designs['testdesigndoc']
        self.assertEqual(doc['language'], 'javascript')
        view = doc['views']['view1']
        self.assertEqual(view['map'], '// couchtest ctdb testdesigndoc1 testview1 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n')
        self.assertEqual(view['reduce'], '// couchtest ctdb testdesigndoc1 view1 reduce\nfunction (keys, values, rereduce) {\n  if (rereduce) {\n    return sum(values);\n  } else {\n    return values.length;\n  }\n}\n')
        view = doc['views']['view2']
        self.assertEqual(view['map'], '// couchtest ctdb testdesigndoc1 view2 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n')
        # ctdb testdesigndoc2
        doc = designs['testdesigndoc2']
        view = doc['views']['view1']
        self.assertEqual(view['map'], '// couchtest ctdb testdesigndoc2 view1 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n')
        # ctdb testindexdoc
        doc = db['index']['testindexdoc']
        self.assertEqual(doc['index1']['fields'], ['document_type'])

        # ctanotherdb
        db = server['ctanotherdb']
        designs = db['designs']
        # ctanotherdb testdesigndoc
        doc = designs['testdesigndoc']
        self.assertEqual(doc['language'], 'javascript')
        view = doc['views']['view']
        self.assertEqual(view['map'], '// couchtest ctanotherdb testdesigndoc view map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n')

        # ctemptydb
        self.assertEqual(server['ctemptydb'], dict())

    def test_collect_merge_schema(self):
        expected_schema = dict(
            default=dict(
                ctemptydb=dict(),
                ctdb=dict(
                    designs=dict(
                        couchtest_testdesigndoc=dict(
                            views=dict(
                                view1=dict(
                                    map='// couchtest ctdb testdesigndoc1 testview1 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n',
                                    reduce='// couchtest ctdb testdesigndoc1 view1 reduce\nfunction (keys, values, rereduce) {\n  if (rereduce) {\n    return sum(values);\n  } else {\n    return values.length;\n  }\n}\n',
                                ),
                                view2=dict(
                                    map='// couchtest ctdb testdesigndoc1 view2 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n',
                                ),
                            ),
                            language='javascript',
                        ),
                        couchtest_testdesigndoc2=dict(
                            views=dict(
                                view1=dict(
                                    map='// couchtest ctdb testdesigndoc2 view1 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n',
                                ),
                            ),
                        ),
                    ),
                    index=dict(
                        couchtest_testindexdoc=dict(
                            index1=dict(fields=['document_type']),
                        ),
                    ),
                ),
                ctanotherdb=dict(
                    designs=dict(
                        couchtest_testdesigndoc=dict(
                            language='javascript',
                            views=dict(
                                view=dict(map='// couchtest ctanotherdb testdesigndoc view map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n'),
                            ),
                        ),
                    ),
                ),
            ),
        )
        schema = collect_schema()
        schema = merge_schema(schema)
        self.assertEqual(schema, expected_schema)


class SchemaTest(CouchTestCase):
    def test_migrate(self):
        server = Server(alias='default')
        server.delete_database_if_exists('ctdb')
        server.delete_database_if_exists('ctanotherdb')
        server.delete_database_if_exists('ctemptydb')
        migrate()
        # ctdb
        db = server.get_database('ctdb')
        data = db.get('_design/couchtest_testdesigndoc')
        self.assertEqual(
            data['views']['view1'],
            dict(
                map='// couchtest ctdb testdesigndoc1 testview1 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n',
                reduce='// couchtest ctdb testdesigndoc1 view1 reduce\nfunction (keys, values, rereduce) {\n  if (rereduce) {\n    return sum(values);\n  } else {\n    return values.length;\n  }\n}\n',
            )
        )
        self.assertEqual(
            data['views']['view2'],
            dict(map='// couchtest ctdb testdesigndoc1 view2 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n')
        )
        data = db.get('_design/couchtest_testdesigndoc2')
        self.assertEqual(
            data['views']['view1'],
            dict(map='// couchtest ctdb testdesigndoc2 view1 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n')
        )
        index = db.get_index('couchtest_testindexdoc', 'index1')
        self.assertEqual(index['ddoc'], 'couchtest_testindexdoc')
        self.assertEqual(index['name'], 'index1')
        self.assertEqual(index['type'], 'json')
        self.assertEqual(index['def'], dict(fields=[dict(document_type='asc')]))
        # ctanotherdb
        db = server.get_database('ctanotherdb')
        data = db.get('_design/couchtest_testdesigndoc')
        self.assertEqual(data['language'], 'javascript')
        self.assertEqual(
            data['views']['view'],
            dict(
                map='// couchtest ctanotherdb testdesigndoc view map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n',
            )
        )
        # ctemptydb
        server.get_database('ctemptydb')

    def test_tescase_migration(self):
        # Design docs specified in couchschema should be present at every CouchTestCase test
        server = Server(alias='default')
        # ctdb
        db = server.get_database('ctdb')
        data = db.get('_design/couchtest_testdesigndoc')
        self.assertEqual(
            data['views']['view1'],
            dict(
                map='// couchtest ctdb testdesigndoc1 testview1 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n',
                reduce='// couchtest ctdb testdesigndoc1 view1 reduce\nfunction (keys, values, rereduce) {\n  if (rereduce) {\n    return sum(values);\n  } else {\n    return values.length;\n  }\n}\n',
            )
        )
        self.assertEqual(
            data['views']['view2'],
            dict(map='// couchtest ctdb testdesigndoc1 view2 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n')
        )
        data = db.get('_design/couchtest_testdesigndoc2')
        self.assertEqual(
            data['views']['view1'],
            dict(map='// couchtest ctdb testdesigndoc2 view1 map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n')
        )
        # ctanotherdb
        db = server.get_database('ctanotherdb')
        data = db.get('_design/couchtest_testdesigndoc')
        self.assertEqual(data['language'], 'javascript')
        self.assertEqual(
            data['views']['view'],
            dict(
                map='// couchtest ctanotherdb testdesigndoc view map\nfunction (doc) {\n  emit(doc._id, 1);\n}\n',
            )
        )
        # ctemptydb
        server.get_database('ctemptydb')
