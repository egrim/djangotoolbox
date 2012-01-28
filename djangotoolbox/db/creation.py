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

        # Standard field types, more or less suitable fora  database
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
        # for SetFields if your database cannot store sets directly,
        # but note that the same set may be represented by different
        # lists (with elements in different order), so lookups may
        # have some quirks if you do so.
        'RawField':                   'raw',
        'BlobField':                  'bytes',
        'AbstractIterableField':      'list',
        'ListField':                  'list',
        'SetField':                   'set',
        'DictField':                  'dict',
        'EmbeddedModelField':         'dict',
    }

    def db_info(self, field):
        """
        Returns a tuple of (field_type, db_type, db_table, db_subinfo)
        containing all info needed to encode field's value for a nonrel
        database. Used by convert_value_to/from_db.

        The first argument is just the field's internal type (kind); it
        is needed to do what missing value_to_db_* methods could do.

        We put db_table alongside field db_type -- to allow back-ends
        having separate key spaces for different tables to create keys
        refering to the right table.

        For list-like fields we also need db_infos of elements and for
        dict-like fields db_infos of values -- the third element of the
        tuple is a callable that can compute the db_info for index or
        key of a value. For untyped collections (with values not tied to
        fields) we do almost no encoding / decoding of elements.

        Consider the following example:

            class Blog(models.Model):
                post = EmbeddedModelField(Post)
                posts = ListField(models.ForeignKey(Post))

            class Post(models.Model)
                pass

        a db_info for the "post" field could be:

            ('EmbeddedModelField', 'dict', 'blog',
                 func('post_id' => ('AutoField', 'key', 'post', None)))

        and for the "posts" field it could be:

            ('ListField', 'list', 'blog',
                 func(0 => ('ForeignKey', 'key', 'post', None)))
        """

        # Memoize the result on the field to improve performance for
        # typed collections (that use just one field for all items).
        if not hasattr(field, '_db_info'):

            # Field type is usually just the base field class name,
            # while db_type is usually expected value's type or "key".
            field_type = field.get_internal_type()
            db_type = self.db_type(field)

            # For ForeignKey, OneToOneField and ManyToManyField use the
            # table of the model the field refers to.
            if field.rel is not None:
                db_table = field.rel.to._meta.db_table
            else:
                try:
                    db_table = field.model._meta.db_table
                except AttributeError:
                    db_table = None

            # Collection fields should provide a value_field method that
            # determines the field a value belongs to, turn it into a
            # method computing db_info for this field.
            if hasattr(field, 'value_field'):
                db_subinfo = lambda *args: self.db_info(
                    field.value_field(*args))
            else:
                db_subinfo = lambda *args: None

            field._db_info = (field_type, db_type, db_table, db_subinfo)

        return field._db_info

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
