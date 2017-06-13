from .default import *
from .warn import configure_warnings

configure_warnings()


INSTALLED_APPS = [
    'couch',
    'couchtest',
    'django.contrib.staticfiles',
]

COUCH_SERVERS = dict(
    default=dict(
        USERNAME='admin',
        PASSWORD='admin',
    ),
)
