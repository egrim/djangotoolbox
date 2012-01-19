import datetime

from django.db.backends import BaseDatabaseFeatures, BaseDatabaseOperations, \
    BaseDatabaseWrapper, BaseDatabaseClient, BaseDatabaseValidation, \
    BaseDatabaseIntrospection

from .creation import NonrelDatabaseCreation


class NonrelDatabaseFeatures(BaseDatabaseFeatures):
    can_return_id_from_insert = True
    supports_unspecified_pk = False
    supports_regex_backreferencing = True
    supports_date_lookup_using_string = False
    supports_timezones = False

    supports_joins = False
    distinguishes_insert_from_update = False
    supports_select_related = False
    supports_deleting_related_objects = False
    string_based_auto_field = False
    supports_dicts = False

    def _supports_transactions(self):
        return False


class NonrelDatabaseOperations(BaseDatabaseOperations):
    """
    Override all database conversions normally done by fields (through
    get_db_prep_value/save/lookup) to be able to pass Python values
    to the database layer. Drivers of NoSQL database either can work
    with Python data directly or need some type-based conversions.
    """
    def __init__(self, connection):
        self.connection = connection
        super(NonrelDatabaseOperations, self).__init__()

    def pk_default_value(self):
        """
        Returns None as a way to ask the back-end to generate a new
        key for an "inserted" object.
        """
        return None

    def quote_name(self, name):
        """
        Does not do any quoting, as it is not needed for most NoSQL
        databases.
        """
        return name

    def prep_for_like_query(self, value):
        """
        Does no conversion. Overriden to be able to use parts of SQL
        query processing code without losing information.
        """
        return value

    def prep_for_iexact_query(self, value):
        """
        Does no conversion. Overriden to be able to use parts of SQL
        query processing code without losing information.
        """
        return value

    def value_to_db_auto(self, value):
        """
        Cast to a string rather than integers for databases
        supporting string based AutoFields.

        TODO: features.has_key_type sounds better.
        """
        if self.connection.features.string_based_auto_field:
            if value is None:
                return None
            return unicode(value)
        return super(NonrelDatabaseOperations, self).value_to_db_auto(value)

    def value_to_db_date(self, value):
        """
        Does not do any conversion, assuming that a date can be stored
        directly. Parent casts to a string here using some arbitrary
        format.
        """
        return value

    def value_to_db_datetime(self, value):
        """
        Does not do any conversion, assuming that a datetime can be
        stored directly. Parent method simply casts to a string here.
        """
        return value

    def value_to_db_time(self, value):
        """
        Does not do any conversion, assuming that a time can be stored
        directly. Parent method simply casts to a string here.
        """
        return value

    def value_to_db_decimal(self, value):
        """
        Does not do any conversion, assuming that a decimal can be
        stored directly. Parent method does a simple string conversion
        (that does not preserve comparisons).
        """
        return value

    def year_lookup_bounds(self, value):
        """
        Converts year bounds to datetime bounds as these can likely be
        used directly, also adds one to the upper bound as database is
        expected to use one strict inequality for between-like filters.
        """
        return [datetime.datetime(value, 1, 1, 0, 0, 0, 0),
                datetime.datetime(value+1, 1, 1, 0, 0, 0, 0)]

    def convert_values(self, value, field):
        """
        Does no conversion, assuming that values returned by the
        database are standard Python types suitable to be passed to
        fields. Parent casts values meant for Integer and Auto fields
        to int and every other value to float here.

        TODO: This method is not even called by the currently existing
        back-ends, however it should be, so conversion/deconversion may
        be done in a symmetric manner.
        """
        return value

    def check_aggregate_support(self, aggregate):
        """
        Defaults to supporting only count.

        TODO: Rather GAE specific, move.
        """
        from django.db.models.sql.aggregates import Count
        if not isinstance(aggregate, Count):
            raise NotImplementedError("This database does not support %r "
                                      "aggregates" % type(aggregate))


class NonrelDatabaseClient(BaseDatabaseClient):
    pass


class NonrelDatabaseValidation(BaseDatabaseValidation):
    pass


class NonrelDatabaseIntrospection(BaseDatabaseIntrospection):
    def table_names(self):
        """Returns a list of names of all tables that exist in the database."""
        return self.django_table_names()


class FakeCursor(object):
    def __getattribute__(self, name):
        raise NotImplementedError('Cursors not supported')

    def __setattr__(self, name, value):
        raise NotImplementedError('Cursors not supported')


class NonrelDatabaseWrapper(BaseDatabaseWrapper):
    # These fake operators are required for SQLQuery.as_sql() support.
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }

    def _cursor(self):
        return FakeCursor()
