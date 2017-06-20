from .default import *
from .warn import configure_warnings

configure_warnings()


INSTALLED_APPS = [
    'couch',
    'couchtest',
    'django.contrib.staticfiles',
]

DATABASES = dict()

COUCH_SERVERS = dict(
    default=dict(
        USERNAME='admin',
        PASSWORD='admin',
    ),
)

TEST_RUNNER = 'couch.test.runner.CouchDiscoverRunner'
