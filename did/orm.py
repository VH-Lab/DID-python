from did import session, Query, DIDDocument, DID, DIDId
from abc import ABC, abstractmethod
from .validate import SUPPORTED_DATATYPE
from .utils import pascal_to_snake_case
from blake3 import blake3  # pylint: disable=no-name-in-module
from .time import current_time
import json
import copy
import os


class QueryOperator:
    @abstractmethod
    def _to_didquery(self, field):
        """
        Convert query state for the given field to a proper did.query statement
        """
        pass


class Equals:
    def __init__(self, value):
        self.value = value

    @abstractmethod
    def _to_didquery(self, field):
        return Query(field) == self.value


class _QueryWrapper:
    def __init__(self, ds,):
        self.ds = ds
        self.query = Query('document_class.class_name') == self.ds.__name__

    def filter(self, filter):
        if not isinstance(session, DID):
            session.init()
        for field in filter:
            self.query = self.query & filter[field]._to_didquery()

    '''
    def all(self):
        results = self.session.find(self.query)
        objs = self._to_obj(results)
        return objs

    
    def _to_obj(self, docs):
        objs = []
        for doc in docs:
            obj_dict, dependencies = self._doc_to_dict(doc)
            dependencies.append(doc)

            obj = self.schema_class.from_diddocument(obj_dict)
            obj._pre_loaded = dependencies
            objs.append(obj)
        return objs

    def _doc_to_dict(self, doc):
        output = {}
        dependencies = {}
        for classname in doc:
            if classname != 'base' and classname != 'class_properties' and classname != 'depends_on':
                for field in doc[classname]:
                    output[field] = doc[classname][field]
        if 'depends_on' in doc:
            for dependency in doc['depends_on']:
                obj = self.session.find_by_id(did_id=dependency['value'])
                dependencies[obj.property_list_name] = obj
                if dependency['associated_field']['type'] == 'obj':
                    output[dependency['associated_field']['name']] = obj
                else:
                    output[dependency['associated_field']['name']
                           ][dependency['associated_field']['location']] = obj
        return output, dependencies
    '''


class _SchemaManager:
    def __init__(self, ds):
        self._ds = ds
        self._ds_type = pascal_to_snake_case(ds.__name__)

    def describe(self, field):
        schema = self.current()
        if field in schema[schema['document_class']['property_list_name']]:
            return schema[schema['document_class']['property_list_name']][field]['description']
        raise AttributeError(
            '{} is not a field defined in the schema'.format(field))

    def current(self):
        from .settings import get_documentpath, get_variable

        try:
            self.init()
        except RuntimeError:
            pass

        file_name = self._ds_type
        if not file_name.endswith('.json'):
            file_name += '.json'

        with open(os.path.join(get_documentpath(), file_name), 'r') as f:
            return json.load(f)

    def history(self):
        from .settings import get_documentpath, get_variable

        versions = []
        try:
            self.init()
        except RuntimeError:
            pass

        file_name = self._ds_type
        if not file_name.endswith('.json'):
            file_name += '.json'

        for fn in os.listdir(os.path.join(os.path.join(get_documentpath(), 'history', file_name[:-5]))):
            if fn.endswith('.json') and fn.startswith('version-'):
                version = int(fn.rstrip('.json').lstrip('version-'))
                file_dir = os.path.join(os.path.join(get_documentpath(
                ), 'history', file_name[:-5]), 'version-{}.json'.format(version))
                with open(file_dir, 'r') as f:
                    versions.append({
                        'version': version,
                        'schema': json.load(f)
                    })

        versions = sorted(versions, key=lambda x: -1 * x['version'])

        latest, _ = self._create_schema()

        versions.insert(0, {
            'version': 'latest (version: {})'.format(versions[0]['version'] + 1) if len(versions) > 0 else "Latest (version: 1)",
            'schema': latest.data
        })
        return versions

    def update(self):
        from .settings import get_documentpath

        try:
            return self.init()
        except RuntimeError:
            pass

        latest, file_name = self._create_schema()

        with open(os.path.join(get_documentpath(), file_name), 'r+') as f:
            schema = json.load(f)
            version = schema['document_class']['class_version']
            if schema[latest.property_list_name] == latest.data[latest.property_list_name]:
                return schema
            latest.data['document_class']['class_version'] = version + 1
            f.seek(0)
            f.truncate()
            json.dump(latest.data, f, indent=6)
        history = os.path.join(os.path.join(get_documentpath(
        ), 'history', file_name[:-5]), 'version-{}.json'.format(version))
        with open(history, 'w') as f:
            json.dump(schema, f, indent=6)
        return latest.data

    def _create_schema(self):
        file_name = self._ds_type
        if not file_name.endswith('.json'):
            file_name += '.json'

        latest = DIDDocument.create_emptydoc(self._ds.__name__, self._ds_type)
        raw_doc = self._ds.document_schema()
        doc = {}
        for field in raw_doc:
            if isinstance(raw_doc[field], dict):
                assert 'type' in raw_doc[field], "the key 'type' is required for the value of the corresponding field {}".format(
                    field)
                doc[field] = {}
                doc[field]['type'] = raw_doc[field]['type'].as_dict()
                doc[field]['description'] = raw_doc[field]['description'] if 'description' in raw_doc[field] else 'no description'
            else:
                doc[field] = {}
                doc[field]['type'] = raw_doc[field].as_dict()
                doc[field]['description'] = 'no description'

        latest.data[latest.property_list_name] = doc
        del latest.data['base']
        return latest, file_name

    def init(self):
        from .settings import get_documentpath, get_variable

        file_name = self._ds_type
        if not file_name.endswith('.json'):
            file_name += '.json'

        if os.path.isfile(os.path.join(os.path.join(get_documentpath(), file_name))) and \
                os.path.isdir(os.path.join(os.path.join(get_documentpath(), 'history', file_name[:-5]))):
            raise RuntimeError(
                "Versioning for class {} has already been initiated".format(file_name[:-5]))

        latest, file_name = self._create_schema()

        with open(os.path.join(get_documentpath(), file_name), 'w') as f:
            json.dump(latest.data, f, indent=6)

        try:
            os.mkdir(os.path.join(os.path.join(get_documentpath(), 'history')))
        except FileExistsError:
            pass

        try:
            os.mkdir(os.path.join(os.path.join(
                get_documentpath(), 'history', file_name[:-5])))
        except FileExistsError:
            pass

        return latest.data


def update_schema(module_name):
    import sys
    import inspect

    module = __import__(module_name)

    for _, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and issubclass(obj, DataStructure) and obj.__name__ != 'DataStrcture':
            try:
                obj.schema.update()
                print("Updated class {}".format(obj.__name__))
            except RuntimeError:
                print("Skipping class {}".format(obj.__name__))


class _DocumentClass:
    def __init__(self, definition, class_name, property_list_name, class_version=1, superclasses=None):
        self._definition = definition
        self._class_name = class_name
        self._property_list_name = property_list_name
        self._class_version = class_version
        self._superclasses = superclasses

    def __repr__(self):
        return json.dumps({
            'definition': self._definition,
            'class_name': self._class_name,
            'property_list_name': self._property_list_name,
            'class_version': self._class_version,
            'superclasses': self._superclasses
        }, indent=6)

    def as_dict(self):
        return {
            'definition': self._definition,
            'class_name': self._class_name,
            'property_list_name': self._property_list_name,
            'class_version': self._class_version,
            'superclasses': self._superclasses
        }

    @property
    def definition(self):
        return self._definition

    @definition.setter
    def definition(self, val):
        raise AttributeError("definition is a read-only property")

    @property
    def class_name(self):
        return self._class_name

    @class_name.setter
    def class_name(self, val):
        raise AttributeError("class_name is a read-only property")

    @property
    def property_list_name(self):
        return self._property_list_name

    @property_list_name.setter
    def property_list_name(self, val):
        raise AttributeError("property_list_name is a read-only property")

    @property
    def class_version(self):
        return self._class_version

    @class_version.setter
    def class_version(self, val):
        raise AttributeError("class_version is a read-only property")

    @property
    def superclasses(self):
        return self._superclasses

    @superclasses.setter
    def superclasses(self, val):
        raise AttributeError("superclasses is a read-only property")


class Meta(type):
    def __new__(cls, name, bases, attrs):
        datastrcture = super().__new__(cls, name, bases, attrs)
        if name != 'DataStructure':
            datastrcture.schema = _SchemaManager(datastrcture)
            datastrcture.query = _QueryWrapper(datastrcture)
            datastrcture.class_properties = _DocumentClass(
                **(datastrcture.schema.update()['document_class'])
            )
            datastrcture._superclasses = bases
        return datastrcture


class _Base():
    def __init__(self, session_id=None, id=None, name=None, datestamp=None, version=1):
        self.session_id = None
        self._id = id if isinstance(id, DIDId) else DIDId().id
        self.name = None
        self.datestamp = current_time() if datestamp is None else datestamp
        self._version = version

    def __repr__(self):
        return json.dumps({
            'session_id': self.session_id,
            'id': self._id,
            'name': self.name,
            'datestamp': self.datestamp,
            'document_version': self._version
        }, indent=6)

    def as_dict(self):
        return {
            'session_id': self.session_id,
            'id': self._id,
            'name': self.name,
            'datestamp': self.datestamp,
            'document_version': self._version
        }

    @property
    def id(self):
        return self.session_id

    @property
    def version(self):
        return self._version

    @id.setter
    def id(self, id):
        raise AttributeError("id cannot be set")

    @version.setter
    def version(self, ver):
        raise AttributeError("version cannot be set")


class DataStructure(metaclass=Meta):
    def __init__(self, **kwargs):
        if type(self) is DataStructure:
            raise NotImplementedError("Schema cannot be instantiated directly")
        else:
            for f, v in kwargs.items():
                setattr(self, f, v)

    @property
    def base(self):
        if not hasattr(self, '_base'):
            self._base = _Base()
        return getattr(self, '_base')

    @classmethod
    def from_dict(cls, kv):
        return cls.__init__(**kv)

    @classmethod
    def schema(cls):
        if hasattr(cls, 'document_type'):
            doc_type = getattr(cls, 'document_type')
        else:
            doc_type = pascal_to_snake_case(getattr(cls, '_class'))
        if not doc_type.endswith('.json'):
            doc_type += '.json'
        path = DIDDocument.where(document_type=doc_type)
        path = os.path.join(path[0], path[1])
        with open(path, 'r') as f:
            schema_file = json.load(f)
        return path, schema_file

    @property
    def db(self):
        if not hasattr(self, '_db'):
            self._db = _DB(self)
        return self._db

    def serialize(self):
        properties = {}
        built_in_properties = {'db', 'query',
                               'base', 'class_properties', 'schema'}
        for field in dir(self):
            if not field.startswith('_') and not callable(getattr(self, field)) and field not in built_in_properties:
                properties[field] = getattr(self, field)
        return properties

    @classmethod
    def _create_schema(cls, obj):
        empty_dict = {}
        for field in obj:
            datatype = list(obj[field].keys())[0]
            options = obj[field][datatype]
            empty_dict[field] = SUPPORTED_DATATYPE[datatype].__init__(
                **options).as_dict()
        return empty_dict

    def to_diddocument(self):

        def create_dependency(type_checker, obj, docs):
            if issubclass(obj, DataStructure):
                dependency = obj.to_diddocument()
                docs.append(dependency)
                dependency_id = dependency.base.id
                return type_checker.generate_depends_on_statement(dependency_id)

        properties = self.serialize()
        property_list_name = getattr(
            self, 'class_properties').property_list_name
        data = {
            'base': self.base.as_dict(),
            'document_class': getattr(self, 'class_properties').as_dict(),
            property_list_name: {},
            'depends_on': []}
        schema = self.document_schema()
        docs = []
        field_in_superclass = {}

        for superclass in getattr(self, '_superclasses'):
            if issubclass(superclass, DataStructure):
                superclass_schema = superclass.document_schema()
                for field in superclass_schema:
                    field_in_superclass[field] = {
                        'property_list_name': superclass.class_properties.property_list_name,
                        'schema': superclass_schema
                    }

        for field in properties:
            if field in schema:
                self._process_field(
                    schema,
                    field,
                    properties,
                    data,
                    property_list_name,
                    create_dependency,
                    docs
                )
            elif field in field_in_superclass:
                self._process_field(
                    field_in_superclass[field]['schema'],
                    field,
                    properties,
                    data,
                    field_in_superclass[field]['property_list_name'],
                    create_dependency,
                    docs
                )
        docs.append(data)
        return docs
        
        '''
        if not hasattr(self, '_doc'):
            self._initialize_did_doc()

        doc = type(self)._doc
        dependencies = [doc]
        type_checker = type(self).schema()[1]

        for field in properties:
            class_associated = None

            if field in doc.data[doc.property_list_name]:
                datatype = list(type_checker[field].keys())[0]
                options = type_checker[field][datatype]
                SUPPORTED_DATATYPE[datatype].__init__(
                    **options).validate(properties[field])
                doc.data[doc.property_list_name][field] = properties[field]
                class_associated = doc.property_list_name
            else:
                for superclass in type(self)._doc.data:
                    if superclass != 'base' and superclass != 'document_class' and superclass != type(self)._doc.property_list_name:
                        try:
                            doc.data[superclass][field] = properties[field]
                            class_associated = superclass
                        except KeyError:
                            pass

            if class_associated:
                if datatype == 'Dict':
                    for key, val in enumerate(properties[field]):
                        if issubclass(type(val), DataStructure):
                            dependency = val.to_diddocument()
                            dependencies.append(dependency)
                            doc['depends_on'].append({
                                'name': type(val).__name__ + '_id',
                                'value': dependency.id,
                                'associated_field': {
                                    'name': field,
                                    'type': 'map',
                                    'location': key
                                }
                            })
                            doc.data[class_associated][field][key] = {
                                '$DEPENDS_ON$': len(doc['depends_on'])-1}
                elif datatype == 'List':
                    for index, item in enumerate(properties[field]):
                        if issubclass(type(item), DataStructure):
                            dependency = val.to_diddocument()
                            dependencies.append(dependency)
                            doc['depends_on'].append({
                                'name': type(item).__name__ + '_id',
                                'value': dependency.id,
                                'associated_field': {
                                    'name': field,
                                    'type': 'list',
                                    'location': index
                                }
                            })
                            doc.data[class_associated][field][index] = {
                                '$DEPENDS_ON$': len(doc['depends_on'])-1}
                elif datatype == 'Object':
                    dependency = val.to_diddocument()
                    dependencies.append(dependency)
                    doc['depends_on'].append({
                        'name': type(item).__name__ + '_id',
                        'value': dependency.id,
                        'associated_field': {
                            'name': field,
                            'type': 'obj'
                        }
                    })
                    doc.data[class_associated][field] = {
                        '$DEPENDS_ON$': len(doc['depends_on'])-1}
        return dependencies
        '''

    def _process_field(self, schema, field, properties, data, property_list_name, create_dependency, docs):
        if isinstance(schema[field], dict):
            type_checker = schema[field]['type']
        else:
            type_checker = schema[field]
        type_checker.validate(properties[field])

        if properties[field] is list:
            data[property_list_name][field] = None * len(properties[field])
            for index, item in enumerate(properties[field]):
                if issubclass(properties[field][index], DataStructure):
                    data[property_list_name][field][index] = '$DEPENDS-ON[{}]$'.format(
                        len(data['depends_on']))
                    data['depends_on'].append(create_dependency(
                        type_checker, properties[field][index], docs))
                else:
                    data[property_list_name][field][index] = item
        elif properties[field] is dict:
            data[property_list_name][field] = {}
            for key, value in properties[field].items():
                if issubclass(properties[field][key], DataStructure):
                    data[property_list_name][field][key] = '$DEPENDS-ON[{}]$'.format(
                        len(data['depends_on']))
                    data['depends_on'].append(create_dependency(
                        type_checker, properties[field][key], docs))
                else:
                    data[property_list_name][field][key] = value
        elif issubclass(properties[field], DataStructure):
            data[property_list_name][field] = '$DEPENDS-ON[{}]$'.format(
                len(data['depends_on']))
            data['depends_on'].append(create_dependency(
                type_checker, properties[field], docs))
        else:
            data[property_list_name][field] = properties[field]

    @classmethod
    def _initialize_did_doc(cls):
        if hasattr(cls, 'document_type'):
            cls._doc = DIDDocument(document_type=getattr(cls, 'document_type'))
        else:
            cls._doc = DIDDocument(
                document_type=pascal_to_snake_case(getattr(cls, '_class')))

    @classmethod
    def document_schema(cls):
        raise NotImplementedError(
            "the class method document_schema needs to be implemented")


class _DB:
    def __init__(self, schema):
        if not isinstance(session, DID):
            self.session = session.init()
        else:
            self.session = session
        self._schema = schema

    def insert(self):
        docs = self._schema.to_diddocument()
        for doc in docs:
            self.session.add(doc)

    def delete(self):
        if not hasattr(self._schema, '_preloaded'):
            raise RuntimeError("DIDDocument must come from the database")
        docs = getattr(self._schema, 'pre_loaded')
        for doc in docs:
            self.session.delete_by_id(did_id=doc.id)

    def update(self):
        if not hasattr(self, '_preloaded'):
            raise RuntimeError("DIDDocument must come from the database")
        _, id = getattr(self, '_preloaded')
        doc = self._schema.to_diddocument()
        doc.data['base']['id'] = id
        self.session.update(doc)
