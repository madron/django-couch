from django.conf import settings
from django.test import TestCase
from .. import Server
from ..utils import migrate

TEST_DATABASE_PREFIX = 't_e_s_t__'


class CouchTestCase(TestCase):
    def _pre_setup(self):
        super(CouchTestCase, self)._pre_setup()
        # Test prefix
        self.change_test_prefix()
        migrate()

    def _post_teardown(self):
        super(CouchTestCase, self)._post_teardown()
        self.delete_test_databases()
        self.revert_test_prefix()

    def change_test_prefix(self):
        for config in settings.COUCH_SERVERS.values():
            prefix = config.get('DATABASE_PREFIX', '')
            if not prefix.startswith(TEST_DATABASE_PREFIX):
                config['DATABASE_PREFIX'] = '{}{}'.format(TEST_DATABASE_PREFIX, prefix)

    def revert_test_prefix(self):
        for config in settings.COUCH_SERVERS.values():
            prefix = config.get('DATABASE_PREFIX', '')
            if prefix.startswith(TEST_DATABASE_PREFIX):
                config['DATABASE_PREFIX'] = prefix.replace(TEST_DATABASE_PREFIX, '', 1)

    def delete_test_databases(self):
        for alias in settings.COUCH_SERVERS.keys():
            server = Server(alias=alias)
            for db_name in server.list_databases():
                server.delete_database(db_name)
