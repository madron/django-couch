import pytz
from datetime import datetime
from django.test import override_settings
from django.test import TestCase
from django.utils import timezone
from .. import exceptions
from .. import fields


class TextFieldTest(TestCase):
    def test_to_json(self):
        value = fields.TextField().to_json('Text')
        self.assertEqual(value, 'Text')

    def test_to_python(self):
        value = fields.TextField().to_python('Text')
        self.assertEqual(value, 'Text')


class DateTimeFieldTest(TestCase):
    def test_to_json_naive(self):
        with self.assertRaises(exceptions.CouchError) as context:
            fields.DateTimeField().to_json(datetime(2013, 5, 1, 12, 0))
        self.assertEqual(context.exception.args, ('Naive datetimes are not supported.',))

    def test_to_json_aware(self):
        # Athens
        data = pytz.timezone('Europe/Athens').localize(datetime(2013, 5, 1, 12, 0))
        value = fields.DateTimeField().to_json(data)
        self.assertEqual(value, '2013-05-01T09:00:00+00:00')
        # Rome
        data = pytz.timezone('Europe/Rome').localize(datetime(2013, 5, 1, 12, 0))
        value = fields.DateTimeField().to_json(data)
        self.assertEqual(value, '2013-05-01T10:00:00+00:00')

    def test_to_json_aware_dst(self):
        # Daylight saving time
        data = pytz.timezone('Europe/Rome').localize(datetime(2013, 2, 1, 12, 0))
        value = fields.DateTimeField().to_json(data)
        self.assertEqual(value, '2013-02-01T11:00:00+00:00')
        # No daylight saving time
        data = pytz.timezone('Europe/Rome').localize(datetime(2013, 8, 1, 12, 0))
        value = fields.DateTimeField().to_json(data)
        self.assertEqual(value, '2013-08-01T10:00:00+00:00')

    def test_to_python_naive(self):
        # Naive datetimes should not be present in couch but, if present, we assume they
        # are stored in utc
        with timezone.override(pytz.utc):
            value = fields.DateTimeField().to_python('2013-05-01T12:00:00')
        self.assertEqual(value, pytz.utc.localize(datetime(2013, 5, 1, 12, 0)))
        pytz.timezone('Europe/Athens').localize(datetime(2013, 5, 1, 12, 0))
        with timezone.override(pytz.timezone('Europe/Athens')):
            value = fields.DateTimeField().to_python('2013-05-01T12:00:00')
        self.assertEqual(value, pytz.timezone('Europe/Athens').localize(datetime(2013, 5, 1, 15, 0)))

    def test_to_python_aware(self):
        with timezone.override(pytz.utc):
            value = fields.DateTimeField().to_python('2013-05-01T12:00:00+00:00')
        self.assertEqual(value, pytz.utc.localize(datetime(2013, 5, 1, 12, 0)))
        with timezone.override(pytz.timezone('Europe/Athens')):
            value = fields.DateTimeField().to_python('2013-05-01T12:00:00+00:00')
        self.assertEqual(value, pytz.timezone('Europe/Athens').localize(datetime(2013, 5, 1, 15, 0)))

    def test_to_python_no_utc(self):
        # Datetime should be stored in utc to permit ordering and filtering in couch.
        # Nethertheless different timezones are correctly parsed
        with timezone.override(pytz.utc):
            value = fields.DateTimeField().to_python('2013-05-01T12:00:00+06:00')
        self.assertEqual(value, pytz.utc.localize(datetime(2013, 5, 1, 6, 0)))
        with timezone.override(pytz.timezone('Europe/Athens')):
            value = fields.DateTimeField().to_python('2013-05-01T12:00:00+06:00')
        self.assertEqual(value, pytz.timezone('Europe/Athens').localize(datetime(2013, 5, 1, 9, 0)))

    @override_settings(TIME_ZONE='Europe/Athens')
    def test_to_python_naive_override(self):
        # Naive datetimes should not be present in couch but, if present, we assume they
        # are stored in utc
        value = fields.DateTimeField().to_python('2013-05-01T12:00:00')
        self.assertEqual(value, pytz.timezone('Europe/Athens').localize(datetime(2013, 5, 1, 15, 0)))

    @override_settings(TIME_ZONE='Europe/Athens')
    def test_to_python_aware_override(self):
        value = fields.DateTimeField().to_python('2013-05-01T12:00:00+00:00')
        self.assertEqual(value, pytz.timezone('Europe/Athens').localize(datetime(2013, 5, 1, 15, 0)))

    @override_settings(TIME_ZONE='Europe/Athens')
    def test_to_python_no_utc_override(self):
        # Datetime should be stored in utc to permit ordering and filtering in couch.
        # Nethertheless different timezones are correctly parsed
        value = fields.DateTimeField().to_python('2013-05-01T12:00:00+06:00')
        self.assertEqual(value, pytz.timezone('Europe/Athens').localize(datetime(2013, 5, 1, 9, 0)))


class JsonFieldTest(TestCase):
    def test_to_json_dict(self):
        data = dict(
            name='Alex Martelli',
            age=61,
            books=[
                dict(title='Python Cookbok'),
                dict(title='Python in a Nutshell'),
            ],
        )
        value = fields.JsonField().to_json(data)
        self.assertEqual(value, data)

    def test_to_json_list(self):
        data = ['red', dict(name='Alex Martelli', age=61), ['cat', 'dog', 28]]
        value = fields.JsonField().to_json(data)
        self.assertEqual(value, data)

    def test_to_python_dict(self):
        data = '{"name": "Alex Martelli", "age": 61, "books": ' \
            '[{"title": "Python Cookbok"}, {"title": "Python in a Nutshell"}]}'
        value = fields.JsonField().to_python(data)
        self.assertEqual(value, data)

    def test_to_python_list(self):
        data = '["red", {"name": "Alex Martelli", "age": 61}, ["cat", "dog", 28]]'
        value = fields.JsonField().to_python(data)
        self.assertEqual(value, data)
