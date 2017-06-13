import json
import requests
from functools import wraps
from django.conf import settings
from . import exceptions

STATUS_CODES_2XX = (200, 201, 202)


class Server(object):
    def __init__(self, alias='default', protocol=None, host=None, port=None, username=None, password=None, database_prefix=None):
        config = settings.COUCH_SERVERS[alias]
        self.alias = alias
        self.protocol = config.get('PROTOCOL', 'http')
        self.host = config.get('HOST', 'localhost')
        self.port = config.get('PORT', 5984)
        self.username = config.get('USERNAME', None)
        self.password = config.get('PASSWORD', None)
        self.database_prefix = config.get('DATABASE_PREFIX', '')
        if protocol is not None:
            self.protocol = protocol
        if host is not None:
            self.host = host
        if port is not None:
            self.port = port
        if username is not None:
            self.username = username
        if password is not None:
            self.password = password
        if database_prefix is not None:
            self.database_prefix = database_prefix
        if self.username and self.password:
            self.auth = (self.username, self.password)
        else:
            self.auth = None
        self.url = '{protocol}://{host}:{port}'.format(protocol=self.protocol, host=self.host, port=self.port)

    def _check_response(self, response, acceptable_status_codes):
        if not response.status_code in acceptable_status_codes:
            result = response.json()
            result['status_code'] = response.status_code
            raise exceptions.CouchError(result)
        return json.loads(response.text)

    def check_connection_error(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except requests.exceptions.ConnectionError as exception:
                data = dict(error='requests.exceptions.ConnectionError', reason=str(exception.args[0]))
                raise exceptions.CouchError(data)
        return wrapped

    @check_connection_error
    def get(self, url, acceptable_status_codes=STATUS_CODES_2XX, **kwargs):
        url = '{}/{}'.format(self.url, url)
        response = requests.get(url, auth=self.auth, **kwargs)
        return self._check_response(response, acceptable_status_codes)

    @check_connection_error
    def post(self, url, acceptable_status_codes=STATUS_CODES_2XX, **kwargs):
        url = '{}/{}'.format(self.url, url)
        response = requests.post(url, auth=self.auth, **kwargs)
        return self._check_response(response, acceptable_status_codes)

    @check_connection_error
    def put(self, url, acceptable_status_codes=STATUS_CODES_2XX, **kwargs):
        url = '{}/{}'.format(self.url, url)
        response = requests.put(url, auth=self.auth, **kwargs)
        return self._check_response(response, acceptable_status_codes)

    @check_connection_error
    def delete(self, url, acceptable_status_codes=STATUS_CODES_2XX, **kwargs):
        url = '{}/{}'.format(self.url, url)
        response = requests.delete(url, auth=self.auth, **kwargs)
        return self._check_response(response, acceptable_status_codes)

    def _cluster_setup(self):
        return self.post('/_cluster_setup', json=dict(action='finish_cluster'))

    def single_node_setup(self):
        try:
            self._cluster_setup()
        except exceptions.CouchError as e:
            if not e.args[0]['reason'] == 'Cluster is already finished':
                raise

    def _all_dbs(self):
        return self.get('/_all_dbs')

    def _get_database_name(self, name):
        return '{}{}'.format(self.database_prefix, name)

    def create_database(self, name):
        from .database import Database
        self.put('/{}'.format(self._get_database_name(name)))
        return Database(name, server=self)

    def get_database(self, name):
        from .database import Database
        self.get('/{}'.format(self._get_database_name(name)))
        return Database(name, server=self)

    def get_or_create_database(self, name):
        from .database import Database
        try:
            self.get_database(name)
        except exceptions.CouchError as e:
            if e.args[0]['error'] == 'not_found':
                return self.create_database(name)
            raise
        return Database(name, server=self)

    def delete_database(self, name):
        return self.delete('/{}'.format(self._get_database_name(name)))

    def delete_database_if_exists(self, name):
        try:
            return self.delete_database(name)
        except exceptions.CouchError as e:
            if e.args[0]['error'] == 'not_found':
                return
            raise

    def list_databases(self):
        databases = [d for d in self._all_dbs() if not d.startswith('_')]
        databases = [d.replace(self.database_prefix, '', 1) for d in databases if d.startswith(self.database_prefix)]
        return databases
