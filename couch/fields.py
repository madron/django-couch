import pytz
from decimal import Decimal
from dateutil.parser import parse
from django.utils.timezone import get_current_timezone
from . import exceptions


class Field(object):
    def __init__(self, default=None):
        self.default = default

    def _to_python(self, value):
        if value is None:
            return value
        return self.to_python(value)

    def _to_json(self, value):
        if value is None:
            return value
        return self.to_json(value)

    def to_python(self, value):
        return str(value)

    def to_json(self, value):
        return str(value)


class TextField(Field):
    pass


class IntegerField(Field):
    def to_python(self, value):
        return int(value)

    def to_json(self, value):
        return int(value)


class FloatField(Field):
    def to_python(self, value):
        return float(value)

    def to_json(self, value):
        return float(value)


class DecimalField(Field):
    def to_python(self, value):
        return Decimal(value)

    def to_json(self, value):
        return str(value)


class BooleanField(Field):
    def to_python(self, value):
        return value

    def to_json(self, value):
        return value


class DateField(Field):
    def to_python(self, value):
        return parse(value).date()

    def to_json(self, value):
        return value.isoformat()


class DateTimeField(Field):
    def to_python(self, value):
        value = parse(value)
        if not value.tzinfo:
            value = pytz.utc.normalize(pytz.utc.localize(value))
        tz = get_current_timezone()
        return value.astimezone(tz)

    def to_json(self, value):
        tzinfo = value.tzinfo
        if not tzinfo:
            raise exceptions.CouchError('Naive datetimes are not supported.')
        value = tzinfo.normalize(value).astimezone(pytz.utc)
        return value.isoformat()


class JsonField(Field):
    def to_python(self, value):
        return value

    def to_json(self, value):
        return value
