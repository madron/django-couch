from django.test.runner import DiscoverRunner
from ..utils import server_setup


class CouchDiscoverRunner(DiscoverRunner):
    def setup_test_environment(self, **kwargs):
        super(CouchDiscoverRunner, self).setup_test_environment(**kwargs)
        server_setup()
