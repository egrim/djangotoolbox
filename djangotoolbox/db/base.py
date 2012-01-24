import datetime

from django.db.backends import BaseDatabaseFeatures, BaseDatabaseOperations, \
    BaseDatabaseWrapper, BaseDatabaseClient, BaseDatabaseValidation, \
    BaseDatabaseIntrospection
from django.utils.functional import Promise


class NonrelDatabaseFeatures(BaseDatabaseFeatures):

    # NoSQL databases usually return a key after saving a new object.
    can_return_id_from_insert = True

    # TODO: Why? Doesn't seem true in general.
    supports_date_lookup_using_string = False
    supports_timezones = False

    # Features that are commonly not available on nonrel databases.
    supports_joins = False
    distinguishes_insert_from_update = False
    supports_select_related = False
    supports_deleting_related_objects = False

    # Does the database use one special type for all keys and references?
    # If set to True, all primary keys, foreign keys and other references
    # will get "key" db_type, otherwise it will be determined using original
    # Django's logic.
    has_key_type = True

    # Can a dictionary be saved / fetched from the database.
    supports_dicts = False

    def _supports_transactions(self):
        return False


class NonrelDatabaseOperations(BaseDatabaseOperations):
    """
    Override all database conversions normally done by fields (through
    get_db_prep_value/save/lookup) to make it possible to pass Python
    values directly to the database layer. On the other hand, provide
    framework for making type-based conversions --  drivers of NoSQL
    database either can work with Python objects directly, sometimes
    representing one type using a another or expect everything encoded
    in some specific manner.

    Django normally handles conversions for the database by providing
    BaseDatabaseOperations.value_to_db_* / convert_values methods,
    but there are some problems with them:
    -- there are no methods for string / integer conversion or for
       nonrel specific fields (e.g. iterables, blobs);
    -- some conversions are not specific to a field kind and can't rely
       on field internal_type (e.g. key conversions);
    -- some standard fields do not call value_to_db_* for each
       operation (e.g. DecimalField only defines get_db_value_save, so
       the conversion is not applied to lookup values).

    Prefer standard methods when the conversion is specific to a
    field kind and these methods when you can convert any value of
    a type.

    Please note, that after changes to type conversions, data saved
    using preexisting methods needs to be handled.
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
        If the database has its own key type it's better to leave any
        conversions to the back-end.
        """
        if self.connection.features.has_key_type:
            return value
        return super(NonrelDatabaseOperations, self).value_to_db_auto(value)

    def value_to_db_date(self, value):
        """
        Does not do any conversion, assuming that a date can be stored
        directly.
        """
        return value

    def value_to_db_datetime(self, value):
        """
        Does not do any conversion, assuming that a datetime can be
        stored directly.
        """
        return value

    def value_to_db_time(self, value):
        """
        Does not do any conversion, assuming that a time can be stored
        directly.
        """
        return value

    def value_to_db_decimal(self, value):
        """
        Does not do any conversion, assuming that a decimal can be
        stored directly.
        """
        return value

    def year_lookup_bounds(self, value):
        """
        Converts year bounds to datetime bounds as these can likely be
        used directly, also adds one to the upper bound as database is
        expected to use one strict inequality for BETWEEN-like filters.
        """
        return [datetime.datetime(value, 1, 1, 0, 0, 0, 0),
                datetime.datetime(value + 1, 1, 1, 0, 0, 0, 0)]

    def convert_values(self, value, field):
        """
        Does no conversion, assuming that values returned by the
        database are standard Python types suitable to be passed to
        fields.
        """
        return value

    def check_aggregate_support(self, aggregate):
        """
        NonrelQueries are only expected to implement COUNT in general.
        """
        from django.db.models.sql.aggregates import Count
        if not isinstance(aggregate, Count):
            raise NotImplementedError('This database does not support %r '
                                      'aggregates' % type(aggregate))

    def convert_value_for_db(self, value, db_info):
        """
        Converts a standard Python value to a type that can be stored
        or processed by the database.

        This implementatin only converts values with "list", "set" or
        "dict" db_type and evaluates lazy objects. You may want to call
        it before doing other back-end specific conversions.

        :param value: A value to be passed to the database driver
        :param db_info: A 3-tuple with (db_type, db_table, db_subinfo)
        """
        db_type, db_table, db_subinfo = db_info or (None, None, None)

        # Force evaluation of lazy objects (e.g. lazy translation strings).
        # Some back-ends pass values directly to the database driver, which
        # may fail if it relies on type inspection and gets a functional proxy.
        # This code relies on __unicode_ cast in django.utils.functional just
        # evaluating the wrapped function and doing nothing more.
        if isinstance(value, Promise):
             value = unicode(value)

        # Convert all values in a list or set using its subtype.
        # We store both as lists on default.
        if db_type == 'list' or db_type == 'set':

            # Note that value for a ListField and alike may be a list
            # element, that should be converted as a single value using
            # the db_subinfo.
            # TODO: What about looking up a list in a list of lists? We
            #       should rather check if it's a lookup or not here.
            if isinstance(value, (list, tuple, set)):
                value = [self.convert_value_for_db(subvalue, db_subinfo)
                         for subvalue in value]
            else:
                value = self.convert_value_for_db(value, db_subinfo)

        # Convert dict values using the db_subtype; also convert
        # non-Mapping types using the db_subinfo (for lookups).
        # TODO: Only values, not keys?
        elif db_type == 'dict':
            if isinstance(value, dict):
                value = dict((key, self.convert_value_for_db(subvalue, db_subinfo))
                              for key, subvalue in value.iteritems())
            else:
                value = self.convert_value_for_db(value, db_subinfo)

        return value

    def convert_value_from_db(self, value, db_info):
        """
        Converts a database type to a standard Python type.

        If you encoded a value for storage in the database, reverse the
        encoding here. This implementation only recuresively deconverts
        elements of iterables (for "list", "set" or "dict" db_type).

        :param value: A value received from the database
        :param db_info: A 3-tuple with (db_type, db_table, db_subinfo)
        """
        db_type, db_table, db_subinfo = db_info or (None, None, None)

        # Deconvert each value in a list, return a set for the set type.
        # Note: Lookup values never get deconverted, so we can skip the
        # the "single value" check here.
        if db_type == 'list' or db_type == 'set':
            value = [self.convert_value_from_db(subvalue, db_subinfo)
                     for subvalue in value]
            if db_type == 'set':
                value = set(value)

        # We may have encoded dict values, so now decode them.
        elif db_type == 'dict':
            value = dict((key, self.convert_value_from_db(subvalue, db_subinfo))
                          for key, subvalue in value.iteritems())

        return value


class NonrelDatabaseClient(BaseDatabaseClient):
    pass


class NonrelDatabaseValidation(BaseDatabaseValidation):
    pass


class NonrelDatabaseIntrospection(BaseDatabaseIntrospection):
    def table_names(self):
        """
        Returns a list of names of all tables that exist in the database.
        """
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
