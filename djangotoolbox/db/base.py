import datetime

from django.db.backends import BaseDatabaseFeatures, BaseDatabaseOperations, \
    BaseDatabaseWrapper, BaseDatabaseClient, BaseDatabaseValidation, \
    BaseDatabaseIntrospection
from django.db.utils import DatabaseError
from django.utils.functional import Promise
from django.utils.importlib import import_module
from django.utils.safestring import EscapeString, EscapeUnicode, SafeString, \
    SafeUnicode

from .creation import NonrelDatabaseCreation


class NonrelDatabaseFeatures(BaseDatabaseFeatures):

    # NoSQL databases usually return a key after saving a new object.
    can_return_id_from_insert = True

    # TODO: Doesn't seem necessary in general, move to back-ends.
    #       Mongo: see PyMongo's FAQ; GAE: see: http://timezones.appspot.com/.
    supports_date_lookup_using_string = False
    supports_timezones = False

    # Features that are commonly not available on nonrel databases.
    supports_joins = False
    distinguishes_insert_from_update = False
    supports_select_related = False
    supports_deleting_related_objects = False

    # Does the database use one type for all keys and references or
    # can any type be used? If set to True, all primary keys, foreign
    # keys and other references will have a "key" db_type, otherwise
    # the db_type set for field kind will be used for type conversions.
    has_single_key_type = True

    # Can primary_key be used on any field? Without encoding usually
    # only a limited set of types is acceptable for keys. This is a set
    # of all field kinds (internal_types) for which the primary_key
    # argument may be used.
    # TODO: Use during model validation.
    # TODO: Move to core and use to skip unsuitable Django tests.
    supports_primary_key_on = set(NonrelDatabaseCreation.data_types.keys()) - \
        set(('ForeignKey', 'RelatedAutoField', 'OneToOneField', 'ManyToManyField',
            'RawField', 'BlobField',))

    # Can a dictionary be saved / fetched from the database.
    # TODO: Remove once "list" is supported for all collection fields.
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
    -- some preparations need to be done for all values or for values
       of a particular "kind" (e.g. lazy objects evaluation or casting
       strings to standard types);
    -- some conversions need more info about the field or model the
       value comes from (e.g. key conversions);
    -- there are no value_to_db_* methods for some value types (bools);
    -- we need to handle nonrel specific fields (collections, blobs).

    Prefer standard methods when the conversion is specific to a
    field kind and the added convert_value_for/from_db methods when you
    can convert any value of a "type".

    Please note, that after changes to type conversions, data saved
    using preexisting methods needs to be handled; and also that Django
    does not expect any special database driver exceptions, so any such
    exceptions should be reraised as django.db.utils.DatabaseError.
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
        Assuming that the database has its own key type, leaves any
        conversions to the back-end.

        Note that Django can pass a string representation of the value
        instead of the value itself (after receiving it as a query
        parameter for example), so you'll likely need to limit
        your AutoFields in a way that makes str(value) reversible.
        """
        return value

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

    def encode_for_db_key(self, value, db_info):
        """
        Converts value to be used as a key to an acceptable type.
        On default we do no encoding, only allowing key values directly
        acceptable by the back-end.

        The conversion has to be reversible given the field type,
        encoding should preserve comparisons.

        Use this to expand the set of fields that can be used as
        primary keys, return value sutiable for a key rather than
        a key itself.
        """
        raise DatabaseError(
            '{0} may not be used as primary key field'.format(db_info[0]))

    def decode_from_db_key(self, value, db_info):
        """
        Decodes value previously encoded for a key.
        """
        return value

    def convert_value_for_db(self, value, db_info, lookup=False):
        """
        Converts a standard Python value to a type that can be stored
        or processed by the database.

        This implementation only converts elements of collections for
        "list", "set" and "dict" db_types, evaluates lazy objects and
        Django's Escape/SafeData and handles embedded models.
        Currently, we assume that dict keys do not require conversion
        (including column, model and module names of embedded models).

        You may want to call this method before doing other back-end
        specific conversions.

        :param value: A value to be passed to the database driver
        :param db_info: A 4-tuple with (field, db_type, db_table,
                        db_subinfo); only use the field for inspection,
                        it may not hold any value or hold a different
                        value than the one you're to convert
        :param lookup: Is the value being prepared as a filter
                       parameter or for storage
        """

        # Back-ends may want to store empty lists or dicts as None.
        if value is None:
            return None

        field, db_type, db_subinfo = db_info or \
           (None, None, None, None)

        # Force evaluation of lazy objects (e.g. lazy translation
        # strings).
        # Some back-ends pass values directly to the database driver,
        # which may fail if it relies on type inspection and gets a
        # functional proxy.
        # This code relies on unicode cast in django.utils.functional
        # just evaluating the wrapped function and doing nothing more.
        if isinstance(value, Promise):
             value = unicode(value)

        # Django wraps strings marked as safe or needed escaping,
        # convert them to just strings for type-inspecting back-ends.
        # See: django.utils.safestring.py.
        if isinstance(value, (SafeString, EscapeString)):
             value = str(value)
        if isinstance(value, (SafeUnicode, EscapeUnicode)):
             value = unicode(value)

        # Convert all elements of a list or set and values of a dict
        # using the proper subinfo,
        # Note that collection field lookup values are single values
        # rather than collections, but they still should be converted
        # using collection's db_subinfo (assuming it's the same for all
        # elements).
        if db_type == 'list':
            if lookup:
                value = self.convert_value_for_db(value, db_subinfo(), lookup)
            else:
                value = list(
                    self.convert_value_for_db(element, db_subinfo(index), lookup)
                    for index, element in enumerate(value))
        elif db_type == 'set':
            if lookup:
                value = self.convert_value_for_db(value, db_subinfo(), lookup)
            else:
                value = set(
                    self.convert_value_for_db(element, db_subinfo(), lookup)
                    for element in value)
        elif db_type == 'dict':
            if lookup:
                # TODO: How do we really know which subinfo to use for lookup value?
                #       We could do a bit more for EmbeddedModelFields.
                value = self.convert_value_for_db(value, db_subinfo(), lookup)
            else:

                # We will save a field.column => value dict, possibly
                # augmented with model info (to be able to deconvert
                # the embedded instance with untyped fields).
                # This can be processed as any other dict in following
                # back-end conversions.
                if field.get_internal_type() == 'EmbeddedModelField':
                    model, field_values = value

                    # Convert using proper field's db_info, change keys
                    # from fields to columns.
                    value = dict(
                        (subfield.column, self.convert_value_for_db(
                            subvalue, db_subinfo(subfield), lookup))
                        for subfield, subvalue in field_values.iteritems())

                    # Store model info alongside values for untyped
                    # embedding; if none is given the field uses a
                    # fixed model.
                    if model is not None:
                        value['_module'] = model.__class__.__module__
                        value['_model'] = model.__class__.__name__

                else:
                    value = dict((key, self.convert_value_for_db(
                            subvalue, db_subinfo(key), lookup))
                        for key, subvalue in value.iteritems())

        return value

    def convert_value_from_db(self, value, db_info):
        """
        Converts a database type to a standard Python type.

        If you encoded a value for storage in the database, reverse the
        encoding here. This implementation only recursively deconverts
        elements of iterables (for "list", "set" or "dict" db_type).

        Note: lookup values never get deconverted.

        You may want to call this method after any back-end specific
        deconversions.

        :param value: A value received from the database
        :param db_info: A 4-tuple with (field, db_type, db_table,
                        db_subinfo)
        """

        # We did not convert Nones.
        if value is None:
            return None

        field, db_type, db_subinfo = db_info or \
           (None, None, None, None)

        # Deconvert elements of a list or set assuming a field that
        # uses such type has an "item_field" property.
        if db_type == 'list':
            value = list(
                self.convert_value_from_db(element, db_subinfo(index))
                for index, element in enumerate(value))

            # Support using "list" for SetField storage.
            if field.get_internal_type() == 'SetField':
                value = set(value)

        elif db_type == 'set':
            value = set(
                self.convert_value_from_db(element, db_subinfo())
                for element in value)

        elif db_type == 'dict':

            # Embedded instance may need to load its model class first,
            # so we know fields to be used for value deconversions.
            if field.get_internal_type() == 'EmbeddedModelField':

                # We either use the model stored alongside the values
                # (untyped embedding) or the one provided by the field
                # (typed embedding).
                module = value.pop('_module', None)
                model = value.pop('_model', None)
                if module is None:
                    model = field.embedded_model
                else:
                    model = getattr(import_module(module), model)

                # Deconvert field values and prepare a dict that can be
                # used to initialize a model. Leave fields for which no
                # value is stored uninitialized.
                data = {}
                for subfield in model._meta.fields:
                    try:
                        data[subfield.attname] = self.convert_value_from_db(
                            value[subfield.column], db_subinfo(subfield))
                    except KeyError:
                        pass

                # Create and return a model instance, so the field
                # doesn't have to use the SubfieldBase metaclass.
                # Note: double underline is not a typo.
                value = model(__entity_exists=True, **data)

            else:
                value = dict(
                    (key, self.convert_value_from_db(subvalue, db_subinfo(key)))
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
