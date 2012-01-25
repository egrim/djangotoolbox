from django.db.backends.creation import BaseDatabaseCreation


class NonrelDatabaseCreation(BaseDatabaseCreation):

    # "Types" used by back-end conversion routines to decide how to
    # convert data for or from the database. Type is understood here
    # a bit differently than in vanilla Django -- it should be read
    # as a identifier of an encoding / decoding procedure to use, 
    # rather than just a database column type.
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
        # TODO: Use unicode rather than text/longtext.
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

        # Mappings for fields provided by nonrel. You may use "list"
        # for SetFields, but not DictFields. TODO: Doesn't seem hard to handle.
        'RawField':          'raw',
        'BlobField':         'blob',
        'AbstractIterableField': 'list',
        'ListField':         'list',
        'SetField':          'set',
        'DictField':         'dict',
        'EmbeddedModelField': 'dict',
    }

    def db_info(self, field):
        """
        Returns a tuple of (db_type, db_table, db_subinfo) containing
        all info needed to encode field's value for a nonrel database.
        Used by convert_value_to/from_db.

        We put db_table alongside field db_type -- to allow back-ends
        having separate key spaces for different tables to create keys
        refering to the right table.

        For collection fields (ListField etc.) we also need db_info
        of elements -- that's the third element of the tuple. Currently
        for untyped collections (with values not tied to fields) we do
        almost no encoding / decoding of elements.

        Consider the following example:

            class Blog(models.Model):
                posts = ListField(models.ForeignKey(Post))

            clas Post(models.Model)
                pass

        a db_info for the 'posts' field could be:

            ('list', 'blog', ('key', 'post', None))
        """

        # For ForeignKey, OneToOneField and ManyToManyField use the
        # table of the model the field refers to.
        if field.rel is not None:
            db_table = field.rel.to._meta.db_table
        else:
            db_table = field.model._meta.db_table

        # Compute db_subinfo from item_field of iterable fields.
        try:
            db_subinfo = self.db_info(field.item_field)
        except AttributeError:
            db_subinfo = None

        return self.db_type(field), db_table, db_subinfo

    def db_type(self, field):
        """
        If the databases has a special key type, returns "key" for
        all primary key fields and related fields independent of the
        field class; otherwise uses original Django's logic.
        """
        if self.connection.features.has_key_type and \
            (field.primary_key or field.rel is not None):
            return 'key'
        return super(NonrelDatabaseCreation, self).db_type(field)

    def related_db_type(self, field):
        """
        TODO: Doesn't seem necessary any longer.
        """
        if self.connection.features.has_key_type:
             return 'key'
        return super(NonrelDatabaseCreation, self).db_type(field)
#        return self.db_type(field)

    def sql_create_model(self, model, style, known_models=set()):
        """
        Most NoSQL databases are mostly schema-less, no data
        definitions are needed.
        """
        return [], {}

    def sql_indexes_for_model(self, model, style):
        """Creates all indexes needed for local fields of a model."""
        return []
