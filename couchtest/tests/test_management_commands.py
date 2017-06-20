from django.core.management import call_command
from django.utils.six import StringIO
from couch import Server
from couch.test import CouchTestCase


class CouchMigrateTest(CouchTestCase):
    def test_verbosity_0(self):
        server = Server(alias='default')
        server.delete_database_if_exists('ctdb')
        server.delete_database_if_exists('ctanotherdb')
        server.delete_database_if_exists('ctemptydb')
        # Command
        out = StringIO()
        call_command('couch_migrate', verbosity=0, stdout=out)
        self.assertEqual(out.getvalue(), '')
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

    def test_verbosity_1(self):
        server = Server(alias='default')
        server.delete_database_if_exists('ctdb')
        server.delete_database_if_exists('ctanotherdb')
        server.delete_database_if_exists('ctemptydb')
        # Command
        out = StringIO()
        call_command('couch_migrate', verbosity=1, stdout=out)
        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 10)
        self.assertIn('Couch migrate started.', lines)
        self.assertIn("Server 'default' setup.", lines)
        self.assertIn("Server 'default' - Database 'ctdb' created.", lines)
        self.assertIn("Server 'default' - Database 'ctdb' - Design document 'couchtest_testdesigndoc' added.", lines)
        self.assertIn("Server 'default' - Database 'ctdb' - Design document 'couchtest_testdesigndoc2' added.", lines)
        self.assertIn("Server 'default' - Database 'ctdb' - Index 'couchtest_testindexdoc/index1' added.", lines)
        self.assertIn("Server 'default' - Database 'ctanotherdb' created.", lines)
        self.assertIn("Server 'default' - Database 'ctanotherdb' - Design document 'couchtest_testdesigndoc' added.", lines)
        self.assertIn("Server 'default' - Database 'ctemptydb' created.", lines)
        self.assertIn('Couch migrate finished.', lines)

    def test_verbosity_1_nothing_to_do(self):
        # Command
        out = StringIO()
        call_command('couch_migrate', verbosity=1, stdout=out)
        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 3)
        self.assertIn('Couch migrate started.', lines)
        self.assertIn("Server 'default' setup.", lines)
        self.assertIn('Couch migrate finished.', lines)

    def test_verbosity_1_delete(self):
        db = Server(alias='default').get_database('ctdb')
        db.create_index(ddoc='couchtest_testindexdoc', name='index2', index=dict(fields=['document_type']))
        db.create_index(ddoc='couchtest_testindexdoc2', name='index2', index=dict(fields=['document_type']))
        # Command
        out = StringIO()
        call_command('couch_migrate', verbosity=1, stdout=out)
        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 5)
        self.assertIn('Couch migrate started.', lines)
        self.assertIn("Server 'default' setup.", lines)
        self.assertIn("Server 'default' - Database 'ctdb' - Design document 'couchtest_testindexdoc2' removed.", lines)
        self.assertIn("Server 'default' - Database 'ctdb' - Index 'couchtest_testindexdoc/index2' removed.", lines)
        self.assertIn('Couch migrate finished.', lines)
