import json
from copy import deepcopy
from django.utils import six
from . import exceptions
from . import Server
from .fields import (
    Field,
    TextField,
    IntegerField,
    FloatField,
    DecimalField,
    BooleanField,
    DateField,
    DateTimeField,
    JsonField,
)

RESERVED_ATTRIBUTES = ['_fields', '_meta']


class Options(object):
    def __init__(self, meta):
        self.server_alias = getattr(meta, 'server_alias', 'default')
        self.database_name = getattr(meta, 'database_name', None)
        self.document_type = getattr(meta, 'document_type', None)
        self.database = None

    def get_database(self):
        if self.database:
            return self.database
        self.database = Server(alias=self.server_alias).get_database(self.database_name)
        return self.database


class Manager(object):
    def __init__(self, document_class):
        self.document_class = document_class

    def get(self, document_id, raw=False):
        db = self.document_class._meta.get_database()
        try:
            data = db.get(document_id)
        except exceptions.CouchError as e:
            if e.args[0]['error'] == 'not_found':
                raise exceptions.ObjectDoesNotExist()
            raise  # pragma: no cover
        document = self.document_class()
        if raw:
            setattr(document, '_raw', deepcopy(data))
        if 'document_type' in data:
            if not data['document_type'] == document.document_type:
                msg = "Type mismatch error: document_type '{}' expected, got '{}'".format(
                    document.document_type, data['document_type']
                )
                raise exceptions.CouchError(msg)
            self.document_type = data.pop('document_type')
        for key, value in data.items():
            if key in document._fields:
                setattr(document, key, document._fields[key]._to_python(value))
            else:
                setattr(document, key, value)
        return document

    def view(self, *args, **kwargs):
        db = self.document_class._meta.get_database()
        kwargs['document_class'] = self.document_class
        return db.view(*args, **kwargs)

    def find(self, *args, **kwargs):
        db = self.document_class._meta.get_database()
        kwargs['document_class'] = self.document_class
        return db.find(*args, **kwargs)

    def find_one(self, *args, **kwargs):
        db = self.document_class._meta.get_database()
        kwargs['document_class'] = self.document_class
        return db.find_one(*args, **kwargs)


class DocumentBase(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(DocumentBase, cls).__new__
        # Also ensure initialization is only performed for subclasses of Model
        # (excluding Document class itself).
        parents = [b for b in bases if isinstance(b, DocumentBase)]
        if not parents:
            return super_new(cls, name, bases, attrs)
        bases = bases + (dict,)
        # Create the class.
        module = attrs.pop('__module__')
        new_attrs = {'__module__': module}
        classcell = attrs.pop('__classcell__', None)
        # It will be needed for python 3.6
        if classcell is not None:  # pragma: no cover
            new_attrs['__classcell__'] = classcell
        new_class = super_new(cls, name, bases, new_attrs)
        attr_meta = attrs.pop('Meta', None)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta
        # Fields
        fields = dict()
        for name in list(attrs.keys()):
            if isinstance(attrs[name], Field):
                field = attrs.pop(name)
                fields[name] = field
                new_class.add_to_class(name, field.default)
        new_class.add_to_class('_fields', fields)
        # Meta
        new_class.add_to_class('_meta', Options(meta))
        # Manager
        manager = Manager(new_class)
        new_class.add_to_class('objects', manager)
        # Exceptions
        new_class.add_to_class('DoesNotExist', exceptions.ObjectDoesNotExist)
        new_class.add_to_class('MultipleObjectsReturned', exceptions.MultipleObjectsReturned)
        # Add all attributes to the class.
        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)
        return new_class

    def add_to_class(cls, name, val):
        setattr(cls, name, val)


class Document(six.with_metaclass(DocumentBase)):
    def __init__(self, **kwargs):
        super(Document, self).__init__()
        self._id = None
        self._rev = None
        for key, value in kwargs.items():
            setattr(self, key, value)
        if self._meta.document_type:
            if getattr(self, 'document_type', None):
                if not self.document_type == self._meta.document_type:
                    msg = "Type mismatch error: document_type '{}' expected, got '{}'".format(
                        self._meta.document_type, self.document_type
                    )
                    raise exceptions.CouchError(msg)
            else:
                self.document_type = self._meta.document_type

    def __str__(self):
        if self._id:
            return '{} {}'.format(self.__class__.__name__, self._id)
        return self.__class__.__name__

    def __repr__(self):
        return str(self._get_data())

    def __bool__(self):
        return bool(self._id)

    def _get_data(self):
        data = dict()
        for key, value in self.__dict__.items():
            if key in self._fields:
                data[key] = self._fields[key]._to_json(getattr(self, key))
            else:
                if not key.startswith('_'):
                    data[key] = value
        if self._id:
            data['_id'] = self._id
        if self._rev:
            data['_rev'] = self._rev
        if self._meta.document_type:
            self.document_type = self._meta.document_type
            data['document_type'] = self.document_type
        return data

    def save(self, revision_mismatch_override=False, only_if_changed=False):
        db = self._meta.get_database()
        data = self._get_data()
        save = True
        if only_if_changed and self._id:
            try:
                prev_data = self.objects.get(self._id, raw=True)._raw
                if '_rev' not in data:
                    prev_data.pop('_rev', None)
                if data == prev_data:
                    save = False
            except exceptions.ObjectDoesNotExist:
                pass
        if save:
            try:
                result = db.post(
                    '',
                    data=json.dumps(data),
                    headers={'Content-Type': 'application/json'},
                )
            except exceptions.CouchError as exception:
                error = exception.args[0]['error']
                if error == 'conflict':
                    if revision_mismatch_override:
                        # read previous revision
                        data = db.get(self._id)
                        self._rev = data['_rev']
                        return self.save()
                    else:
                        new_exception = exceptions.RevisionMismatch()
                        new_exception.args = exception.args
                        raise new_exception
                raise
            self._id = result['id']
            self._rev = result['rev']
            return 'saved'
        return 'unchanged'

    def delete(self):
        db = self._meta.get_database()
        url = '{}?rev={}'.format(self._id, self._rev)
        db.delete(url)


class DesignDocument(Document):
    language = TextField(default='javascript')
    views = JsonField()

    def save(self, **kwargs):
        kwargs['only_if_changed'] = True
        return super(DesignDocument, self).save(**kwargs)
