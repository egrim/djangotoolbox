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

    def db_type(self, field):
        """
        If the databases has a special type used for all keys, returns
        "key" for all primary key fields and related fields independent
        of the field class; otherwise uses the original Django's logic.
        """
        if self.connection.features.has_single_key_type and \
            (field.primary_key or field.rel is not None):
            return 'key'
        return super(NonrelDatabaseCreation, self).db_type(field)

    def related_db_type(self, field):
        """
        TODO: Doesn't seem necessary any longer. Also remove all
              references to "RelatedAutoField".
        """
        if self.connection.features.has_single_key_type:
             return 'key'
        return super(NonrelDatabaseCreation, self).db_type(field)

    def sql_create_model(self, model, style, known_models=set()):
        """
        Most NoSQL databases are mostly schema-less, no data
        definitions are needed.
        """
        return [], {}

    def sql_indexes_for_model(self, model, style):
        """Creates all indexes needed for local fields of a model."""
        return []
