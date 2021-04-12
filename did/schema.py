from did import session, Query, DIDDocument, DID

class _QueryWrapper:
    def __init__(self, schema, schema_class):
        self.schema_class = schema_class
        self.session = session
        self.document_class = schema
        self._query = Query("document_class.class_name") == schema.lower()

    def _to_schema(self, did_document):
        obj = self.schema_class.super().__init__()
        for field in did_document.property_list_name:
            setattr(
                obj, field, did_document.data[did_document.property_list_name][field])
        for superclass in did_document.superclasses:
            _diddocument = DIDDocument(document_type=superclass['definition'])
            for field in _diddocument.property_list_name:
                setattr(
                    obj, field, did_document.data[_diddocument.property_list_name][field])
        return obj

    @property
    def query(self):
        return self._query

    def all(self):
        docs = self.session.find(self._query)
        for i in range(len(docs)):
            docs[i] = self._to_schema(docs[i])
        return docs

    def and_(self, ndi_query):
        self._query = self._query.add_(ndi_query)

    def __and__(self, ndi_query):
        self._query = self._query.add_(ndi_query)

    def or_(self, ndi_query):
        self._query = self._query.or_(ndi_query)

    def __or__(self, ndi_query):
        self._query = self._query.or_(ndi_query)

    def equals(self, value):
        self._query = self._query.equals(value)

    def __eq__(self, value):
        self._query = self._query.equals(value)

    def __ne__(self, value):
        self._query = self._query.__ne__(value)

    def contains(self, value):
        self._query = self._query.contains(value)

    def match(self, value):
        self._query = self._query.match(value)

    def greater_than(self, value):
        self._query = self._query.greater_than(value)

    def __gt__(self, value):
        self._query = self._query.greater_than(value)

    def greater_than_or_equal_to(self, value):
        self._query = self._query.greater_than_or_equal_to(value)

    def __ge__(self, value):
        self._query = self._query.greater_than_or_equal_to(value)

    def less_than(self, value):
        self._query = self._query.less_than(value)

    def __lt__(self, value):
        self._query = self._query.less_than(value)

    def less_than_or_equal_to(self, value):
        self._query = self._query.less_than_or_equal_to(value)

    def __le__(self, value):
        self._query = self._query.less_than_or_equal_to(value)

    def exists(self, value=True):
        self._query = self._query.exists(value)

    def __pos__(self):
        self._query = self._query.exists(True)

    def __neg__(self):
        self._query = self._query.exists(False)

    def in_(self, value):
        self._query = self._query.in_(value)

    def __rshift__(self, value):
        self._query = self._query.__rshift__(value)


class Meta(type):
    def __new__(cls, name, bases, attrs):
        schema = super().__new__(cls, name, bases, attrs)
        schema._class = name
        schema._superclasses = bases
        schema._id = None
        schema.query = _QueryWrapper(name, schema)
        return schema


class Schema(metaclass=Meta):
    def __init__(self):
        if type(self) is Schema:
            raise NotImplementedError("Schema cannot be instantiated directly")

    @property
    def db(self):
        if not hasattr(self, '_db'):
            self._db = _DB(self)
        return self._db


class _DB:
    def __init__(self, schema):
        if not isinstance(session, DID):
            self.session = session.init()
        else:
            self.session = session
        self._schema = schema
        try:
            self._diddocument = DIDDocument(document_type=self._schema.document_type)
        except:
            self._diddocument = DIDDocument(document_type=self._schema._class)
        if getattr(self._schema, '_id', None):
            self._diddocument.data['base']['id'] = getattr(self._schema, '_id')

    def _to_diddocument(self):
        for field in self._diddocument.data[self._diddocument.property_list_name]:
            print(field)
            self._diddocument.data[self._diddocument.property_list_name][field] = getattr(
                self._schema, field, None)
        #TODO ignore base as a superclass
        '''
        for superclass in self._diddocument.superclasses:
            _diddocument = DIDDocument(document_type=superclass['definition'])
            for field in _diddocument.property_list_name:
                self._diddocument.data[_diddocument.property_list_name][field] = getattr(
                    self._schema, field, None)
        '''

    def insert(self):
        self._to_diddocument()
        self.session.add(self._diddocument)

    def delete(self):
        self._to_diddocument()
        self.session.delete_by_id(did_id=self._diddocument.id)

    def update(self):
        self._to_diddocument()
        self.session.update(self._diddocument)
