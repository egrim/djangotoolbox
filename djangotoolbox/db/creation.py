from django.db.backends.creation import BaseDatabaseCreation


class NonrelDatabaseCreation(BaseDatabaseCreation):

    # "Types" used by back-end conversion routines to decide how to
    # convert data for or from the database. Type is understood here
    # a bit differently than in vanilla Django -- it should be read
    # as an identifier of an encoding / decoding procedure to use,
    # rather than just a database column type.
    data_types = {

        # NoSQL databases usually have specific concepts of keys. For
        # example, GAE has the db.Key class, MongoDB driver only allows
        # ObjectIds, Redis uses strings, while Cassandra supports
        # different types (including binary data).
        'AutoField':                  'key',
        'RelatedAutoField':           'key',
        'ForeignKey':                 'key',
        'OneToOneField':              'key',
        'ManyToManyField':            'key',

        # Standard field types, more or less suitable for a database
        # (or its client / driver) being able to directly store or
        # process Python objects.
        'BigIntegerField':            'long',
        'BooleanField':               'bool',
        'CharField':                  'string',
        'CommaSeparatedIntegerField': 'string',
        'DateField':                  'date',
        'DateTimeField':              'datetime',
        'DecimalField':               'decimal',
        'EmailField':                 'string',
        'FileField':                  'string',
        'FilePathField':              'string',
        'FloatField':                 'float',
        'ImageField':                 'string',
        'IntegerField':               'integer',
        'IPAddressField':             'string',
        'NullBooleanField':           'bool',
        'PositiveIntegerField':       'integer',
        'PositiveSmallIntegerField':  'integer',
        'SlugField':                  'string',
        'SmallIntegerField':          'integer',
        'TextField':                  'string',
        'TimeField':                  'time',
        'URLField':                   'string',
        'XMLField':                   'string',

        # Mappings for fields provided by nonrel. You may use "list"
        # for SetFields, or even DictField and EmbeddedModelField (if
        # your database supports nested lists), but note that the same
        # set or dict may be represented by different lists (with
        # elements in different order), so order of such data is
        # undetermined.
        'RawField':                   'raw',
        'BlobField':                  'bytes',
        'AbstractIterableField':      'list',
        'ListField':                  'list',
        'SetField':                   'set',
        'DictField':                  'dict',
        'EmbeddedModelField':         'dict',
    }

    def nonrel_db_type(self, field):
        """
        Returns "key" for all primary key and foreign key fields
        independent of the field's own logic, for non key fields
        uses the original Django's db_type logic.

        Note: we can't simply redefine db_type here because we may want
        to override db_type a field may return directly.

        TODO: Field.db_type (as of 1.3.1) is used mostly for generating
              SQL statements (through a couple of methods in
              DatabaseCreation and DatabaseOperations.field_cast_sql)
              or within back-end implementations -- nonrel is not
              dependend on any of these; but there are two cases that
              might need to be fixed, namely:
              -- management/createcachetable (calls field.db_type),
              -- and contrib/gis (defines its own geo_db_type method).

        TODO: related_db_type and related changes are now only needed
              for "legacy" storage methods. At some point also remove
              all instances of "RelatedAutoField".
        """
        if field.primary_key or field.rel is not None:
            return 'key'
        return field.db_type()

    def sql_create_model(self, model, style, known_models=set()):
        """
        Most NoSQL databases are mostly schema-less, no data
        definitions are needed.
        """
        return [], {}

    def sql_indexes_for_model(self, model, style):
        """Creates all indexes needed for local fields of a model."""
        return []
