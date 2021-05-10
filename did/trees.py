from .orm import DataStructure
from .document import DIDDocument

class DocumentTree:
    def __init__(self, ds):
        self.ds = {
            'ds': ds,
            'schema': {},
            'kv': {}
        }
        self._gather_superclasses(self.ds['kv'], self.ds['schema'], type(ds))
        self.children = []
        self.data = {
            'base': ds.base._to_dict(),
            'class_properties': ds.class_properties._to_dict()
        }

        for superclass in self.ds['kv']:
            for field in self.ds['kv'][superclass]:
                # validate the data type
                self.ds['schema'][superclass][field].validate(self.ds['kv'][superclass][field])
                if isinstance(field, list):
                    self.data[superclass][field] = self._handle_list(self.ds['kv'][superclass][field])
                elif isinstance(field, dict):
                    self.data[superclass][field] = self._handle_dict(self.ds['kv'][superclass][field])
                elif isinstance(field, DataStructure):
                    self.data[superclass][field] = self._handle_obj(self.ds['kv'][superclass][field])
                else:
                    self.data[superclass][field] = self.ds['kv'][superclass][field]

        self.data = DIDDocument(data=self.data)

    def construct(self):
        pass

    def update(self, field, value):
        pass

    def _gather_superclasses(self, properties, schema, class_name):
        if issubclass(class_name, DataStructure) and class_name.__name__ != 'DataStrcture':
            properties[class_name.class_properties.property_list_name] = class_name.serialize(self)
            schema[class_name.class_properties.property_list_name] = class_name.document_schema()
            if class_name._superclasses:
                for superclass in class_name._superclasses:
                    self._gather_superclasses(properties, schema, superclass)
        
    def _handle_list(self, val):
        output = None * len(val)
        for index, item in enumerate(val):
            if isinstance(item, list):
                output[index] = self._handle_list(val[index])
            elif isinstance(item, dict):
                output[index] = self._handle_dict(val[index])
            elif issubclass(item, DataStructure):
                output[index] = self._handle_obj(val[index])
            else:
                output[index] = val[index]
        return output
    
    def _handle_dict(self,val):
        output = {}
        for index, item in enumerate(val):
            if isinstance(item, list):
                output[index] = self._handle_list(val[index])
            elif isinstance(item, dict):
                output[index] = self._handle_dict(val[index])
            elif issubclass(item, DataStructure):
                output[index] = self._handle_obj(val[index])
            else:
                output[index] = val[index]
        return output

    def _handle_obj(self, val):
        if not issubclass(val, DataStructure):
            raise ValueError("Object not serializable: must be an instance of did.orm.DataStrcture")
        doc_tree = DocumentTree(val)
        doc_tree.data.data['parent_id'] = self.data.id
        self.children.append(doc_tree)
        if 'depends_on' not in self.data:
            self.data['depends_on'] = []
        self.data['depends_on'].append(
            {
                'definition': '{}_id'.format(doc_tree.data.class_name),
                'value': doc_tree.data.id
            }
        )
        return '$DEPENDS_ON[{}]$'.format(len(self.data['depends_on'])-1)
    
    def __iter__(self):
        # Base Case
        if not self.children:
            yield self.data
        # Dependencies traversed first
        else:
            for child in self.children:
                yield child.__iter__()

