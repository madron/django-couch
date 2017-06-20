from django.test import override_settings
from django.test import SimpleTestCase
from .. import exceptions
from .. import Server
from ..test import CouchTestCase


class ServerTest(CouchTestCase):
    def test_database(self):
        server = Server()
        self.assertNotIn('mydb', server.list_databases())
        # Create database
        server.create_database('mydb')
        # Check database list
        self.assertIn('mydb', server.list_databases())

    def test_create_database(self):
        server = Server()
        db = server.create_database('acme')
        self.assertEqual(db.name, 'acme')
        self.assertEqual(db.server.alias, 'default')

    def test_get_database_not_found(self):
        server = Server()
        with self.assertRaises(exceptions.CouchError):
            server.get_database('acme')

    def test_get_database_ok(self):
        server = Server()
        server.create_database('acme')
        db = server.get_database('acme')
        self.assertEqual(db.name, 'acme')
        self.assertEqual(db.server.alias, 'default')

    def test_get_or_create_database_existing(self):
        server = Server()
        server.create_database('acme')
        db = server.get_or_create_database('acme')
        self.assertEqual(db.name, 'acme')
        self.assertEqual(db.server.alias, 'default')

    def test_get_or_create_database_not_existing(self):
        server = Server()
        db = server.get_or_create_database('acme')
        self.assertEqual(db.name, 'acme')
        self.assertEqual(db.server.alias, 'default')

    @override_settings(COUCH_SERVERS=dict(default=dict(HOST='nowhere.example.com')))
    def test_get_or_create_database_ko(self):
        server = Server()
        with self.assertRaises(exceptions.CouchError):
            server.get_or_create_database('acme')

    def test_list_databases(self):
        server = Server()
        self.assertNotIn('mydb', server.list_databases())
        server.create_database('mydb')
        self.assertIn('mydb', server.list_databases())
        # Databases created without prefix are not listed
        server.put('/a_l_r_e_a_d_y___p_r_e_s_e_n_t____d_b')
        self.assertNotIn('a_l_r_e_a_d_y___p_r_e_s_e_n_t____d_b', server.list_databases())
        server.delete('/a_l_r_e_a_d_y___p_r_e_s_e_n_t____d_b')

    def test_delete_database(self):
        server = Server()
        self.assertNotIn('mydb', server.list_databases())
        server.create_database('mydb')
        server.delete_database('mydb')
        self.assertNotIn('mydb', server.list_databases())

    def test_delete_database_not_found(self):
        server = Server()
        self.assertNotIn('mydb', server.list_databases())
        with self.assertRaises(exceptions.CouchError) as context:
            server.delete_database('mydb')
        self.assertEqual(
            context.exception.args[0],
            dict(reason='Database does not exist.', error='not_found', status_code=404)
        )

    def test_delete_database_if_exists_1(self):
        server = Server()
        server.get_or_create_database('mydb')
        server.delete_database_if_exists('mydb')
        self.assertNotIn('mydb', server.list_databases())

    def test_delete_database_if_exists_2(self):
        server = Server()
        self.assertNotIn('mydb', server.list_databases())
        server.delete_database_if_exists('mydb')
        self.assertNotIn('mydb', server.list_databases())

    @override_settings(COUCH_SERVERS=dict(default=dict(HOST='nowhere.example.com')))
    def test_delete_database_if_exists_ko(self):
        server = Server()
        with self.assertRaises(exceptions.CouchError) as context:
            server.delete_database_if_exists('mydb')
        self.assertEqual(context.exception.args[0]['error'], 'requests.exceptions.ConnectionError')

    def test_single_node_setup(self):
        server = Server()
        server.single_node_setup()

    @override_settings(COUCH_SERVERS=dict(default=dict(HOST='nowhere.example.com')))
    def test_single_node_setup_ko(self):
        server = Server()
        with self.assertRaises(exceptions.CouchError):
            server.single_node_setup()


class DefaultServerTest(SimpleTestCase):
    @override_settings(COUCH_SERVERS=dict(default=dict()))
    def test_default(self):
        server = Server()
        self.assertEqual(server.alias, 'default')
        self.assertEqual(server.protocol, 'http')
        self.assertEqual(server.host, 'localhost')
        self.assertEqual(server.port, 5984)
        self.assertEqual(server.username, None)
        self.assertEqual(server.password, None)
        self.assertEqual(server.database_prefix, '')

    @override_settings(COUCH_SERVERS=dict(default=dict(PROTOCOL='https', HOST='192.168.1.1', PORT=9999, USERNAME='user', PASSWORD='pass', DATABASE_PREFIX='test_')))
    def test_config(self):
        server = Server()
        self.assertEqual(server.alias, 'default')
        self.assertEqual(server.protocol, 'https')
        self.assertEqual(server.host, '192.168.1.1')
        self.assertEqual(server.port, 9999)
        self.assertEqual(server.username, 'user')
        self.assertEqual(server.password, 'pass')
        self.assertEqual(server.database_prefix, 'test_')

    @override_settings(COUCH_SERVERS=dict(default=dict(PROTOCOL='https', HOST='192.168.1.1', PORT=9999, USERNAME='user', PASSWORD='pass', DATABASE_PREFIX='test_')))
    def test_override(self):
        server = Server(protocol='http', host='couch.example.com', port=8888, username='admin', password='admin', database_prefix='demo_')
        self.assertEqual(server.alias, 'default')
        self.assertEqual(server.protocol, 'http')
        self.assertEqual(server.host, 'couch.example.com')
        self.assertEqual(server.port, 8888)
        self.assertEqual(server.username, 'admin')
        self.assertEqual(server.password, 'admin')
        self.assertEqual(server.database_prefix, 'demo_')


class OtherServerTest(SimpleTestCase):
    @override_settings(COUCH_SERVERS=dict(another=dict()))
    def test_no_default_server(self):
        with self.assertRaises(KeyError):
            Server()

    @override_settings(COUCH_SERVERS=dict(default=dict(), another=dict()))
    def test_default(self):
        server = Server(alias='another')
        self.assertEqual(server.alias, 'another')
        self.assertEqual(server.protocol, 'http')
        self.assertEqual(server.host, 'localhost')
        self.assertEqual(server.port, 5984)
        self.assertEqual(server.username, None)
        self.assertEqual(server.password, None)
        self.assertEqual(server.database_prefix, '')

    @override_settings(COUCH_SERVERS=dict(default=dict(), another=dict(PROTOCOL='https', HOST='192.168.1.1', PORT=9999, USERNAME='user', PASSWORD='pass', DATABASE_PREFIX='test_')))
    def test_config(self):
        server = Server(alias='another')
        self.assertEqual(server.alias, 'another')
        self.assertEqual(server.protocol, 'https')
        self.assertEqual(server.host, '192.168.1.1')
        self.assertEqual(server.port, 9999)
        self.assertEqual(server.username, 'user')
        self.assertEqual(server.password, 'pass')
        self.assertEqual(server.database_prefix, 'test_')

    @override_settings(COUCH_SERVERS=dict(default=dict(), another=dict(PROTOCOL='https', HOST='192.168.1.1', PORT=9999, USERNAME='user', PASSWORD='pass', DATABASE_PREFIX='test_')))
    def test_override(self):
        server = Server(alias='another', protocol='http', host='couch.example.com', port=8888, username='admin', password='admin', database_prefix='demo_')
        self.assertEqual(server.alias, 'another')
        self.assertEqual(server.protocol, 'http')
        self.assertEqual(server.host, 'couch.example.com')
        self.assertEqual(server.port, 8888)
        self.assertEqual(server.username, 'admin')
        self.assertEqual(server.password, 'admin')
        self.assertEqual(server.database_prefix, 'demo_')
