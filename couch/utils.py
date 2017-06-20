import os
import pkgutil
import sys
from copy import deepcopy
from importlib import import_module
from django.apps import apps
from django.utils.module_loading import module_has_submodule
from . import documents
from . import Server


def server_setup(verbosity=0, stdout=sys.stdout):
    from django.conf import settings
    for alias in settings.COUCH_SERVERS.keys():
        server = Server(alias=alias)
        server.single_node_setup()
        if verbosity > 0:
            stdout.write("Server '{}' setup.".format(alias))


def collect_schema():
    schema = dict()
    for app_name, app_config in apps.app_configs.items():
        if module_has_submodule(app_config.module, 'couchschema'):
            app = dict()
            couchschema_module = import_module('.couchschema', app_name)
            for server_info in pkgutil.iter_modules(path=couchschema_module.__path__):
                server = dict()
                server_alias = server_info[1]
                server_module = import_module('.{}'.format(server_alias), couchschema_module.__name__)
                for db_info in pkgutil.iter_modules(path=server_module.__path__):
                    db = dict()
                    db_name = db_info[1]
                    db_module = import_module('.{}'.format(db_name), server_module.__name__)
                    # designs
                    designs = deepcopy(getattr(db_module, 'designs', None))
                    if designs:
                        for design in designs.values():
                            for view in design.get('views', dict()).values():
                                for attr in ['map', 'reduce']:
                                    file_name = view.get(attr, None)
                                    if file_name:
                                        file_path = os.path.join(db_module.__path__[0], file_name)
                                        with open(file_path, 'r') as f:
                                            view[attr] = f.read()
                        db['designs'] = designs
                    # index
                    if hasattr(db_module, 'index'):
                        db['index'] = db_module.index
                    # db end
                    server[db_name] = db
                app[server_alias] = server
            schema[app_name] = app
    return schema


def merge_schema(schema):
    merged = dict()
    for app_name, app in schema.items():
        for server_alias, server in app.items():
            if not server_alias in merged:
                merged[server_alias] = dict()

            for db_name, db in server.items():
                if not db_name in merged[server_alias]:
                    merged[server_alias][db_name] = dict()
                # designs
                renamed_designs = None
                if 'designs' in db:
                    renamed_designs = dict()
                    for design_name, design in db['designs'].items():
                        new_design_name = '{}_{}'.format(app_name, design_name)
                        renamed_designs[new_design_name] = design
                if not renamed_designs is None:
                    if 'designs' not in merged[server_alias][db_name]:
                        merged[server_alias][db_name]['designs'] = dict()
                    for name, design in renamed_designs.items():
                        merged[server_alias][db_name]['designs'][name] = design
                # index
                renamed_index = None
                if 'index' in db:
                    renamed_index = dict()
                    for design_name, design in db['index'].items():
                        new_design_name = '{}_{}'.format(app_name, design_name)
                        renamed_index[new_design_name] = design
                if not renamed_index is None:
                    if 'index' not in merged[server_alias][db_name]:
                        merged[server_alias][db_name]['index'] = dict()
                    for name, design in renamed_index.items():
                        merged[server_alias][db_name]['index'][name] = design
    return merged


def apply_schema_migration(schema, verbosity=0, stdout=sys.stdout):
    if schema:
        for alias, server_info in schema.items():
            server = Server(alias=alias)
            for db_name, db_schema in server_info.items():
                db, created = server.get_or_create_database(db_name)
                if created and verbosity > 0:
                    stdout.write("Server '{}' - Database '{}' created.".format(alias, db_name))
                # Remove no more needed design docs
                needed = list(db_schema.get('designs', dict()).keys())
                needed += list(db_schema.get('index', dict()).keys())
                for row in db.list_design_documents()['rows']:
                    design_name = row['id'].replace('_design/', '', 1)
                    if not design_name in needed:
                        url = '{}?rev={}'.format(row['id'], row['value']['rev'])
                        db.delete(url)
                        if verbosity > 0:
                            stdout.write("Server '{}' - Database '{}' - Design document '{}' removed.".format(alias, db_name, design_name))
                # Create design docs
                for design_name, design_schema in db_schema.get('designs', dict()).items():
                    _id = '_design/{}'.format(design_name)
                    doc = documents.DesignDocument(_id=_id, **design_schema)
                    doc._meta.database = db
                    result = doc.save()
                    if result == 'saved' and verbosity > 0:
                        stdout.write("Server '{}' - Database '{}' - Design document '{}' added.".format(alias, db_name, design_name))
                # Remove no more needed indexes
                needed = [(None, '_all_docs')]
                for design_name, index_schema in db_schema.get('index', dict()).items():
                    for index_name, index in index_schema.items():
                        needed.append((design_name, index_name))
                for key in db.list_indexes().keys():
                    if not key in needed:
                        db.delete_index(*key)
                        if verbosity > 0:
                            design_name, index_name = key
                            stdout.write("Server '{}' - Database '{}' - Index '{}/{}' removed.".format(alias, db_name, design_name, index_name))
                # Create indexes
                for design_name, index_schema in db_schema.get('index', dict()).items():
                    for index_name, index in index_schema.items():
                        data = db.create_index(ddoc=design_name, name=index_name, index=index)
                        if data['result'] == 'created' and verbosity > 0:
                            stdout.write("Server '{}' - Database '{}' - Index '{}/{}' added.".format(alias, db_name, design_name, index_name))


def migrate(verbosity=0, stdout=sys.stdout):
    schema = collect_schema()
    schema = merge_schema(schema)
    apply_schema_migration(schema, verbosity=verbosity, stdout=stdout)
