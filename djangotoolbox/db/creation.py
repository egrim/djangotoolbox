from django.db.backends.creation import BaseDatabaseCreation


class NonrelDatabaseCreation(BaseDatabaseCreation):
    # These "types" are used by back-end conversion routines to decide
    # how to convert data for or from the database. Type is here an
    # identifier of an encoding / decoding procedure to use.
    data_types = {

        # NoSQL databases usually have specific concepts of keys. For
        # example, GAE has the db.Key class, MongoDB driver only allows
        # ObjectIds, Redis uses strings, while Cassandra supports
        # different types (including binary data).
        'AutoField':         'key',
        'RelatedAutoField':  'key',
        'ForeignKey':        'key',
        'OneToOneField':     'key',
        'ManyToManyField':   'key',

        # Standard fields types, more or less suitable for databases
        # (or its client / driver) being able to directly store or
        # process Python objects, but having separate types for storing
        # short and long strings.
        'BigIntegerField':   'long',
        'BooleanField':      'bool',
        'CharField':         'text',
        'CommaSeparatedIntegerField': 'text',
        'DateField':         'date',
        'DateTimeField':     'datetime',
        'DecimalField':      'decimal',
        'EmailField':        'text',
        'FileField':         'text',
        'FilePathField':     'text',
        'FloatField':        'float',
        'ImageField':        'text',
        'IntegerField':      'integer',
        'IPAddressField':    'text',
        'NullBooleanField':  'bool',
        'PositiveIntegerField': 'integer',
        'PositiveSmallIntegerField': 'integer',
        'SlugField':         'text',
        'SmallIntegerField': 'integer',
        'TextField':         'longtext',
        'TimeField':         'time',
        'URLField':          'text',
        'XMLField':          'longtext',

         # Mappings for fields provided by nonrel.
        'BlobField':         'blob',
        'RawField':          'raw',
    }

    def db_type(self, field):
        """
        If the databases has a special key type, return "key" for all
        primary key fields independent of the field class, otherwise
        use original Django's logic.
        """
        if self.connection.features.has_key_type and field.primary_key:
            return 'key'
        return super(NonrelDatabaseCreation, self).db_type(field)

    def related_db_type(self, field):
        """
        If the databases has a special key type, use "key" db_type for
        foreign keys and other references.
        """
        if self.connection.features.has_key_type:
            return 'key'
        return super(NonrelDatabaseCreation, self).db_type(field)

    def sql_create_model(self, model, style, known_models=set()):
        """
        Most NoSQL databases are schema-less, no data definitions are
        needed.
        """
        return [], {}

    def sql_indexes_for_model(self, model, style):
        """Creates all indexes needed for local fields of a model."""
        return []
