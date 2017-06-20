from django.core.management.base import BaseCommand
from couch.utils import migrate
from couch.utils import server_setup


class Command(BaseCommand):
    def handle(self, *args, **options):
        verbosity = options.get('verbosity', 0)
        if verbosity > 0:
            self.stdout.write('Couch migrate started.')
        server_setup(verbosity=verbosity, stdout=self.stdout)
        migrate(verbosity=verbosity, stdout=self.stdout)
        if verbosity > 0:
            self.stdout.write('Couch migrate finished.')
