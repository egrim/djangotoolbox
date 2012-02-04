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
            'RawField', 'BlobField', 'AbstractIterableField', 'ListField',
            'SetField', 'DictField', 'EmbeddedModelField'))

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
    values directly to the database layer. On the other hand, provide a
    framework for making type-based conversions --  drivers of NoSQL
    database either can work with Python objects directly, sometimes
    representing one type using a another or expect everything encoded
    in some specific manner.

    Django normally handles conversions for the database by providing
    BaseDatabaseOperations.value_to_db_* / convert_values methods,
    but there are some problems with them:
    -- some preparations need to be done for all values or for values
       of a particular "kind" (e.g. lazy objects evaluation or casting
       strings wrappers to standard types);
    -- some conversions need more info about the field or model the
       value comes from (e.g. key conversions, embedded deconversion);
    -- there are no value_to_db_* methods for some value types (bools);
    -- we need to handle collecion fields (list, set, dict) and they
       need to differentiate between deconverting from database and
       deserializing (so to_python is not enough).

    Prefer standard methods when the conversion is specific to a
    field kind and the added value_for/from_db methods when you
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

    def value_for_db(self, value, field, field_kind, db_type, lookup):
        """
        Converts a standard Python value to a type that can be stored
        or processed by the database driver.

        This implementation only converts elements of iterables passed
        by collection fields, evaluates Django's lazy objects and
        marked strings and handles embedded models.
        Currently, we assume that dict keys and column, model, module
        names (strings) of embedded models require no conversion.

        We need to know the field for two reasons:
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
        :param db_type: Same as creation.nonrel_db_type(field)
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

        # Convert elements of collection fields.
        if field_kind in ('ListField', 'SetField', 'DictField',):
            value = self.value_for_db_collection(value, field,
                                                 field_kind, db_type, lookup)

        # Store model instance fields' values.
        elif field_kind == 'EmbeddedModelField':
            value = self.value_for_db_model(value, field,
                                            field_kind, db_type, lookup)

        return value

    def value_from_db(self, value, field, field_kind, db_type):
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
        :param db_type: Same as creation.nonrel_db_type(field)

        Note: lookup values never get deconverted.
        """

        # We did not convert Nones.
        if value is None:
            return None

        # Deconvert items or values of a collection field.
        if field_kind in ('ListField', 'SetField', 'DictField',):
            value = self.value_from_db_collection(value, field,
                                                  field_kind, db_type)

        # Reinstatiate a serialized model.
        elif field_kind == 'EmbeddedModelField':
            value = self.value_from_db_model(value, field,
                                             field_kind, db_type)

        return value

    def value_for_db_collection(self, value, field, field_kind, db_type, lookup):
        """
        Recursively converts values from AbstractIterableFields.

        We base the conversion on field class / kind and assume some
        knowledge about field internals (that the field has an
        "item_field" property that gives the right subfield for any of
        its values), to avoid adding a framework for determination of
        parameters for items' conversions; we do the conversion here
        rather than inside get_db_prep_save/lookup for symetry with
        deconversion (which can't be in to_python because the method is
        also used for deserialization).

        Note that collection lookup values are plain values rather than
        lists, sets or dicts, but they still should be converted as a
        collection item (assuming all items or values are converted in
        the same way).

        Returns a list, set or dict according to the db_type given. If
        the "list" db_type used for DictField, a list with keys and
        values interleaved will be returned (list of pairs is not good,
        because lists / tuples may need conversion themselves; the list
        may still be nested for dicts containing collections). If an
        unknown db_type is specified, returns a generator yielding
        converted elements / pairs with converted values.
        """
        subfield = field.item_field
        subkind = subfield.get_internal_type()
        db_subtype = self.connection.creation.nonrel_db_type(subfield)

        # Do convert filter parameters.
        if lookup:
            value = self.value_for_db(value, subfield,
                                      subkind, db_subtype, lookup)

        # Convert list/set items or dict values.
        else:
            if field_kind == 'DictField':

                # Generator yielding pairs with converted values.
                value = (
                    (key, self.value_for_db(subvalue, subfield,
                                            subkind, db_subtype, lookup))
                    for key, subvalue in value.iteritems())

                # Return just a dict, a flattened list, or a generator.
                if db_type == 'dict':
                    value = dict(value)
                elif db_type == 'list':
                    value = list(item for pair in value for item in pair)

            else:

                # Generator producing converted items.
                value = (
                    self.value_for_db(subvalue, subfield,
                                      subkind, db_subtype, lookup)
                    for subvalue in value)

                # "list" may be used for SetField.
                if db_type == 'list':
                    value = list(value)
                elif db_type == 'set':
                    # assert field_kind != 'ListField'
                    value = set(value)

        return value

    def value_from_db_collection(self, value, field, field_kind, db_type):
        """
        Recursively deconverts values for AbstractIterableFields.

        Assumes that all values in a collection can be deconverted
        using a single field (Field.item_field, possibly a RawField).

        Returns a value in a format proper for the field kind (the
        value will normally not go through to_python).
        """
        subfield = field.item_field
        subkind = subfield.get_internal_type()
        db_subtype = self.connection.creation.nonrel_db_type(subfield)

        if field_kind == 'DictField':

            # Generator yielding pairs with deconverted values,
            # from a dict or flat list with keys values interleaved.
            if db_type == 'list':
                value = zip(value[::2], value[1::2])
            else:
                value = value.iteritems()

            # DictField needs to hold a dict.
            value = dict(
                (key, self.value_from_db(subvalue, subfield,
                                         subkind, db_subtype))
                for key, subvalue in value)
        else:

            # Generator yielding deconverted items.
            value = (
                self.value_from_db(subvalue, subfield,
                                   subkind, db_subtype)
                for subvalue in value)

            # The value will be available from the field without any
            # further processing and it has to have the right type.
            if field_kind == 'ListField':
                value = list(value)
            elif field_kind == 'SetField':
                value = set(value)

        return value

    def value_for_db_model(self, value, field, field_kind, db_type, lookup):
        """
        Converts a tuple of (embedded_instance, field => value mapping,
        and info whether or not to save model info) received from an
        EmbeddedModelField to a dict or list for storage.

        The embedded instance fields' values are also converted /
        deconverted using value_for/from_db, so any back-end
        conversions will be applied.
        This is the right thing to do, but was not done in the past, so
        if you somehow ended relying on subfield conversions not being
        made change EmbeddedModelFields to LegacyEmbeddedModelFields to
        avoid them (it should not be necessary unless you have lookups
        on EmbeddedModelFields or do some low-level processing using
        values held by their fields).

        Returns (field.column, value) pairs as a dict or a
        once-flattened list, possibly augmented with model info (to be
        able to deconvert the embedded instance for untyped fields).
        Note that just a single level of the list is flattened, so it
        still may be nested (when the embedded instance holds
        collection fields). If an unknown db_type is used a generator
        yielding such pairs will be returned.

        TODO: How should EmbeddedModelField lookups work?
        """
        if lookup:
            # raise NotImplementedError('Needs specification')
            return value

        embedded_instance, field_values, save_model_info = value

        # Convert using proper instance field's info, change keys from
        # fields to columns.
        value = (
            (subfield.column,
             self.value_for_db(subvalue, subfield,
                               subfield.get_internal_type(),
                               self.connection.creation.nonrel_db_type(
                                   subfield),
                               lookup))
            for subfield, subvalue in field_values.iteritems())

        # Embedded instance fields used not to be converted.
        if field.__class__.__name__ == 'LegacyEmbeddedModelField':
            value = ((f.column, v) for f, v in field_values.iteritems())

        # For untyped embedding save model info alongside field values.
        if save_model_info:
            value = list(value) + [
                ('_module', embedded_instance.__class__.__module__),
                ('_model', embedded_instance.__class__.__name__)]

        # Process "dict" or "list" (with columns and their values
        # interleaved) storage types, or return a generator.
        if db_type == 'dict':
            value = dict(value)
        elif db_type == 'list':
            value = list(item for pair in value for item in pair)

        return value

    def value_from_db_model(self, value, field, field_kind, db_type):
        """
        Reinstatiates a serialized model.

        Embedded instances are stored as a (column, value) pairs in a
        dict or single-flattened list; possibly augmented with model
        class info.

        Also handles "old" LegacyEmbeddedModelField, supporting data
        from pre-0.2 Mongo engine.

        Creates and returns a model instance, so the field doesn't have
        to use the SubfieldBase metaclass.
        """

        # List storage -- separate and create a dict.
        if db_type == 'list':
            value = dict(zip(value[::2], value[1::2]))

        # Compatibility with old LegacyEmbeddedModelField (model info
        # used to be serialized differently before 0.2).
        # TODO: Maybe its OK to remove this already?
        if field.__class__.__name__ == 'LegacyEmbeddedModelField':
            value.pop('_app', None)
            if '_module' not in value:
                value.pop('_model', None)
            if '_id' in value:
                value['id'] = value.pop('_id')

        # We either use the model stored alongside values (untyped
        # embedding) or the one from the field (typed embedding).
        # Try the stored values first to support fixing type.
        module = value.pop('_module', None)
        model = value.pop('_model', None)
        if module is not None and model is not None:
            model = getattr(import_module(module), model)
        else:
            model = field.embedded_model

        # Embedded instance fields used not to be deconverted.
        if field.__class__.__name__ == 'LegacyEmbeddedModelField':
            return model(__entity_exists=True, **dict(
                (f.attname, value[f.column]) for f in model._meta.fields
                if (f.column in value)))

        # Deconvert field values and prepare a dict that can be used to
        # initialize a model. Leave fields for which no value is stored
        # uninitialized.
        data = {}
        for subfield in model._meta.fields:
            try:
                data[subfield.attname] = self.value_from_db(
                    value[subfield.column], subfield,
                    subfield.get_internal_type(),
                    self.connection.creation.nonrel_db_type(subfield))
            except KeyError:
                pass

        # Note: the double underline is not a typo -- this lets the
        # field know that the object already exists in the database.
        return model(__entity_exists=True, **data)

    def value_for_db_key(self, value, field_kind):
        """
        Converts value to be used as a key to an acceptable type.
        On default we do no encoding, only allowing key values directly
        acceptable by the database for its key type (if any).

        The conversion has to be reversible given the field type,
        encoding should preserve comparisons.

        Use this to expand the set of fields that can be used as
        primary keys, return value suitable for a key rather than
        a key itself.
        """
        raise DatabaseError(
            '{0} may not be used as primary key field'.format(field_kind))

    def value_from_db_key(self, value, field_kind):
        """
        Decodes a value previously encoded for a key.
        """
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
