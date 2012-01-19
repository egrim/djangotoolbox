from django.db.backends.creation import BaseDatabaseCreation


class NonrelDatabaseCreation(BaseDatabaseCreation):
    # This dictionary maps Field objects to back-end types that should
    # be used to store their values. Type strings can contain format
    # strings; they'll be interpolated against the values of
    # Field.__dict__ before being output. If a type is set to
    # None, it won't be included in the output.
    data_types = {

        # NoSQL databases usually have specific concepts of keys. For
        # example, GAE has the db.Key class, MongoDB driver only allows
        # ObjectIds, Redis uses strings, while Cassandra supports
        # different types (including binary data).
        'AutoField':         'key',
        'ForeignKey':        'key',
        'OneToOneField':     'key',
        'RelatedAutoField':  'key',

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
        Use "key" type for primary key fields independent of the field
        class, for other fields use the original Django logic.
        """
        if field.primary_key:
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
