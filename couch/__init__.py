from django.apps import AppConfig
from .database import Database
from .server import Server


default_app_config = 'couch.CouchConfig'


class CouchConfig(AppConfig):
    name = 'couch'
    verbose_name = 'Couch'
