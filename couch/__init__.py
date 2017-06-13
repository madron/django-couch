from django.apps import AppConfig
from django.db.models.signals import pre_migrate
from . import callbacks
from .database import Database
from .server import Server


default_app_config = 'couch.CouchConfig'


class CouchConfig(AppConfig):
    name = 'couch'
    verbose_name = 'Couch'

    def ready(self):
        pre_migrate.connect(callbacks.server_setup, sender=self)
