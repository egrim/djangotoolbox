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
    # TODO: Remove, unless someone can find a database that really
    #       can't handle dicts (using serialization or nested lists).
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
        Returns None, to be interpreted by back-ends as a request to
        generate a new key for an "inserted" object.
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

    def encode_for_db_key(self, value, field_kind):
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
            '{0} may not be used as primary key field'.format(field_kind))

    def decode_from_db_key(self, value, field_kind):
        """
        Decodes value previously encoded for a key.
        """
        return value

    def convert_value_for_db(self, value, field, field_kind, db_type, lookup):
        """
        Converts a standard Python value to a type that can be stored
        or processed by the database driver.

        This implementation only converts elements of iterables passed
        by collection fields, evaluates Django's lazy objects and
        marked strings and handles embedded models.
        Currently, we assume that dict keys and column, model, module
        names (strings) of embedded models require no conversion.

        We need field for two reasons:
        -- to allow back-ends having separate key spaces for different
           tables to create keys refering to the right table (which can
           be the field model's table or the table of the model of the
           instance a ForeignKey or other relation field points to).
        -- to know the field of values passed by typed collection
           fields and to use the proper fields when deconverting values
           stored for typed embedding field.
        Avoid using the field in any other way than by inspecting its
        properties, it may not hold any value or hold a value other
        than the one you're asked to convert.

        You may want to call this method before doing other back-end
        specific conversions.

        :param value: A value to be passed to the database driver
        :param field: A field having the same properties as the field
                      the value comes from
        :param field_kind: Equal to field.get_internal_type()
        :param db_type: Same as field.db_type()
        :param lookup: Is the value being prepared as a filter
                       parameter or for storage
        """

        # Back-ends may want to store empty lists or dicts as None.
        if value is None:
            return None

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
        if isinstance(value, (SafeString, EscapeString)):
             value = str(value)
        elif isinstance(value, (SafeUnicode, EscapeUnicode)):
             value = unicode(value)

        # Convert elements of collection fields -- we base this on
        # field class / kind to avoid adding a heavy framework for
        # determination of parameters for items' conversions.
        if field_kind in ('ListField', 'SetField', 'DictField',):
            subfield = field.item_field
            subkind = subfield.get_internal_type()
            db_subtype = subfield.db_type(connection=self.connection)

            # Collection field lookup values are plain values rather
            # than collections, but they still should be converted as
            # a collection item (assuming all items or values are
            # converted in the same way).
            if lookup:
                value = self.convert_value_for_db(value, subfield, subkind,
                                                  db_subtype, lookup)

            # Create a generator yielding processed items or pairs with
            # processed subvalues, use it to produce a collection of
            # the requested type. If an unknown db_type is specified,
            # passes the generator to the back-end.
            else:
                if field_kind == 'DictField':
                    value = (
                        (key, self.convert_value_for_db(subvalue, subfield,
                                                        subkind, db_subtype,
                                                        lookup))
                        for key, subvalue in value.iteritems())

                    # Allow a dict or a flat list with keys and values
                    # interleaved to be used for storage (list of pairs
                    # is not enough because tuples may need conversion).
                    if db_type == 'list':
                        value = list(item for pair in value for item in pair)
                    elif db_type == 'dict':
                        value = dict(value)

                else:
                    value = (
                        self.convert_value_for_db(subvalue, subfield, subkind,
                                                  db_subtype, lookup)
                        for subvalue in value)

                    # Cast to the type requested by the back-end.
                    if db_type == 'list':
                        value = list(value)
                    elif db_type == 'set':
                        # assert field_kind != 'ListField'
                        value = set(value)

        # We will save field.column => value pairs as a dict or list,
        # possibly augmented with model info (to be able to deconvert
        # the embedded instance for untyped fields).
        # The resulting dict or list can be processed as any other in
        # following back-end conversions.
        elif field_kind == 'EmbeddedModelField':

            # TODO: How should EmbeddedModelField lookups work?
            if lookup:
                return value
                # raise NotImplementedError('Needs specification')

            embedded_instance, field_values, save_model_info = value

            # Convert using proper instance field's info, change keys
            # from fields to columns.
            value = (
                (subfield.column,
                 self.convert_value_for_db(subvalue, subfield,
                                           subfield.get_internal_type(),
                                           subfield.db_type(
                                               connection=self.connection),
                                           lookup))
                for subfield, subvalue in field_values.iteritems())

            # For untyped embedding store model info alongside field
            # values.
            if save_model_info:
                value = list(value) + [
                    ('_module', embedded_instance.__class__.__module__),
                    ('_model', embedded_instance.__class__.__name__)]

            # Allow dict or flat list (with columns and values
            # interleaved) as storage types.
            if db_type == 'list':
                value = list(item for pair in value for item in pair)
            elif db_type == 'dict':
                value = dict(value)

        return value

    def convert_value_from_db(self, value, field, field_kind, db_type):
        """
        Converts a database type to a type acceptable by the field.

        If you encoded a value for storage in the database, reverse the
        encoding here. This implementation only recursively deconverts
        elements of collection fields and handles embedded models.

        You may want to call this method after any back-end specific
        deconversions.

        :param value: A value to be passed to the database driver
        :param field: A field having the same properties as the field
                      the value comes from
        :param field_kind: Equal to field.get_internal_type()
        :param db_type: Same as field.db_type()

        Note: lookup values never get deconverted.
        """

        # We did not convert Nones.
        if value is None:
            return None

        # Deconvert items or values of a collection field and cast to
        # the format expected by the field (the value will normally not
        # go through to_python).
        if field_kind in ('ListField', 'SetField', 'DictField',):
            subfield = field.item_field
            subkind = subfield.get_internal_type()
            db_subtype = subfield.db_type(connection=self.connection)

            if field_kind == 'DictField':
                if db_type == 'list':
                    value = zip(value[::2], value[1::2])
                else:
                    value = value.iteritems()

                value = dict(
                    (key, self.convert_value_from_db(subvalue, subfield,
                                                     subkind, db_subtype))
                    for key, subvalue in value)
            else:
                value = (
                    self.convert_value_from_db(subvalue, subfield,
                                               subkind, db_subtype)
                    for subvalue in value)

                if field_kind == 'ListField':
                    value = list(value)
                elif field_kind == 'SetField':
                    value = set(value)

        # Embedded instances are stored as a (column, value) dict or
        # flattened list; possibly augmented with model class info.
        elif field_kind == 'EmbeddedModelField':
            if db_type == 'list':
                value = dict(zip(value[::2], value[1::2]))

            # We either use the model stored alongside the values
            # (untyped embedding) or the one provided by the field
            # (typed embedding).
            # Try the stored values first to support fixing type.
            module = value.pop('_module', None)
            model = value.pop('_model', None)
            if module is not None and model is not None:
                model = getattr(import_module(module), model)
            else:
                model = field.embedded_model

            # Deconvert field values and prepare a dict that can be
            # used to initialize a model. Leave fields for which no
            # value is stored uninitialized.
            data = {}
            for subfield in model._meta.fields:
                try:
                    data[subfield.attname] = self.convert_value_from_db(
                        value[subfield.column], subfield,
                        subfield.get_internal_type(),
                        subfield.db_type(connection=self.connection))
                except KeyError:
                    pass

            # Create and return a model instance, so the field doesn't
            # have to use the SubfieldBase metaclass.
            # Note: double underline is not a typo -- let the field
            # know that the object already exists in the database.
            value = model(__entity_exists=True, **data)

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
