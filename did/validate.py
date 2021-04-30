from abc import abstractmethod, ABC
import re

SUPPORTED_DATATYPE = None

class DataType(ABC):
    """
    An abstract based case representing the data type of a DIDDocumentField
    """
    @abstractmethod
    def validate(self, value):
        """
        Perform validation against the value passed in
        """
        pass

    @abstractmethod
    def as_dict(self):
        """
        return a dictionary representation of the datatype which will be written to the 
        schema file when specifying the expected data type
        """
        pass

class Any(DataType):
    def __init__(self):
        pass

    def validate(self, value):
        return True

    def as_dict(self):
        return {'Any': {}}
    
class StringType(DataType):
    def __init__(self, min_length=None, max_length=None, regex=None):
        self.min_length = min_length 
        self.max_length = max_length
        self.regex = re.compile(regex) if regex else None

    def validate(self, value):
        if not isinstance(value, str):
            raise ValueError("expected {}, got {} instead".format(str, type(value)))
        if self.min_length:
            if len(value) < self.min_length:
                raise ValueError("expected a str with a length >= {}".format(self.min_length))
        if self.max_length:
            if len(value) > self.min_length:
                raise ValueError("expected a str with a length <= {}".format(self.max_length))
        if self.regex:
            if not self.regex.match(value):
                raise ValueError("expected the str matches with the regular expression {}".format(self.regex))
    
    def as_dict(self):
        output = {'String' : {}}
        if self.min_length:
            output['String']['min_length'] = self.min_length 
        if self.max_length:
            output['String']['max_length'] = self.max_length 
        if self.regex:
            #make sure we have a valid regular expression
            re.compile(self.regex)
            output['String']['regex'] = self.regex
        return output

class NumberType(DataType):
    def __init__(self, min=None, max=None, discrete_value=None):
        self.min = min
        self.max = max 
        self.discrete_value = discrete_value
    
    def validate(self, value):
        if type(self).__name__ == 'IntegerType':
            if not isinstance(value, int):
                raise ValueError("expected {}, got {} instead".format(int, type(value)))
        else:
            if not isinstance(value, int) and not isinstance(value, float):
                raise ValueError("expected {} or {}, got {} instead".format(float, int, type(value)))
        
        if self.discrete_value is not None:
            if value not in self.discrete_value:
                raise ValueError("expected one of {}".format(self.discrete_value))
        if self.min is not None:
            if value < self.min:
                raise ValueError("expected an int >= {}".format(self.min))
        if self.max is not None:
            if value > self.max:
                raise ValueError("expected an int <= {}".format(self.max))

    def as_dict(self):
        if self.discrete_value:
            if not isinstance(self.discrete_value, list):
                raise ValueError("discrete_value option needs to be a list")
            for val in self.discrete_value:
                if type(self).__name__ == 'IntegerType':
                    if not isinstance(val, int):
                        raise ValueError("individual item in discrete value needs to be int, got {} instead".format(val))
                else:
                    if not isinstance(val, int) and not isinstance(val, float):
                        raise ValueError("individual item in discrete value needs to be float or int, got {} instead".format(val))
            return {'Integer': {'discrete_value' : self.discrete_value}}
        else:
            output = {'Integer': {}}
            if self.min:
                output['Integer']['min'] = self.min
            if self.max:
                output['Integer']['max'] = self.max
        return output

class IntegerType(NumberType):
    pass

class ListType(DataType):
    def __init__(self, max_length=None, min_length=None, length=None, datatypes=None):
        self.length = length
        self.min_length = min_length
        self.max_length = max_length
        self.datatypes = datatypes

    def get(self, index):
        if self.datatypes is None:
            return Any
        elif isinstance(self.datatypes, dict):
            if index in self.datatypes:
                return self.datatypes[index]
            else:
                return Any
        elif isinstance(self.datatypes, list):
            if index < len(self.datatypes):
                return self.datatypes[index]
            else:
                return Any
        else:
            return self.datatypes
    
    def validate(self, value):
        if not isinstance(value, list):
            raise ValueError("expected {}, got {} instead".format(list, type(value)))
        if self.length is not None:
            if len(value) != self.length:
                raise ValueError("expected a list with length = {}, got {} instead".format(self.length, len(value)))
        if self.min_length is not None:
            if len(value) < self.min_length:
                raise ValueError("expected a list with length >= {}, got {} instead".format(self.min_length, len(value)))
        if self.max_length is not None:
            if len(value) > self.max_length:
                raise ValueError("expected a list with length >= {}, got {} instead".format(self.max_length, len(value)))
        if self.datatypes is not None:
            if isinstance(self.datatypes, dict):
                if len(self.datatypes) == 1 and list(self.datatypes.keys())[0] in SUPPORTED_DATATYPE:
                    datatype = list(self.datatypes.keys())[0]
                    options = self.datatypes[datatype]
                    for item in value:
                        SUPPORTED_DATATYPE[datatype](**options).validate(item)
                else:
                    for index in range(len(value)):
                        if index in self.datatypes:
                            datatype = list(self.datatypes[index].keys())[0]
                            options = self.datatypes[index][datatype]
                            SUPPORTED_DATATYPE[datatype](**options).validate(value[index])
            elif isinstance(self.datatypes, list):
                for i in range(len(value)):
                    if i < len(self.datatypes):
                        datatype = list(self.datatypes[i].keys())[0]
                        options = self.datatypes[i][datatype]
                        SUPPORTED_DATATYPE[datatype](**options).validate(value)
            else:
                raise ValueError("datatypes should be either a list or dictionary")
    
    def as_dict(self):
        output = {'List': {}}
        if self.length is not None:
            output['List']['length'] = self.length
        else:
            if self.min_length is not None:
                output['List']['min_length'] = self.min_length
            if self.max_length is not None:
                output['List']['max_length'] = self.max_length
        if self.datatypes is not None:
            if not isinstance(self.datatypes, dict) and not isinstance(self.datatypes, list):
                raise ValueError("datatypes need to be either a dict or list")
            output['List']['datatypes'] = self.datatypes

class DictType(DataType):
    def __init__(self, max_length=None, min_length=None, length=None, datatypes=None):
        self.length = length
        self.min_length = min_length
        self.max_length = max_length
        self.datatypes = datatypes
    
    def validate(self, value):
        if not isinstance(value, list):
            raise ValueError("expected {}, got {} instead".format(list, type(value)))
        if self.length is not None:
            if len(value) != self.length:
                raise ValueError("expected a list with length = {}, got {} instead".format(self.length, len(value)))
        if self.min_length is not None:
            if len(value) < self.min_length:
                raise ValueError("expected a list with length >= {}, got {} instead".format(self.min_length, len(value)))
        if self.max_length is not None:
            if len(value) > self.max_length:
                raise ValueError("expected a list with length >= {}, got {} instead".format(self.max_length, len(value)))
        if self.datatypes is not None:
            if isinstance(self.datatypes, dict):
                if len(self.datatypes) == 1 and list(self.datatypes.keys())[0] in SUPPORTED_DATATYPE:
                    datatype = list(self.datatypes.keys())[0]
                    options = self.datatypes[datatype]
                    for key in value:
                        SUPPORTED_DATATYPE[datatype](**options).validate(value[key])
                else:
                    for key in value:
                        if key in self.datatypes:
                            datatype = list(self.datatypes[key].keys())[0]
                            options = self.datatypes[key][datatype]
                            SUPPORTED_DATATYPE[datatype](**options).validate(value[key])
            else:
                raise ValueError("datatypes should be either a list or dictionary")
    
    def as_dict(self):
        output = {'Map': {}}
        if self.length is not None:
            output['Map']['length'] = self.length
        else:
            if self.min_length is not None:
                output['Map']['min_length'] = self.min_length
            if self.max_length is not None:
                output['Map']['max_length'] = self.max_length
        if self.datatypes is not None:
            if not isinstance(self.datatypes, dict):
                raise ValueError("datatypes need to be either a dict or list")
            output['Map']['datatypes'] = self.datatypes

class ObjectType(DataType):
    def __init__(self, cls=None, definition=None, class_name=None):
        self.cls = cls 
        self.class_name = class_name
        self.definition = definition

    def validate(self, value):
        value._initialize_did_doc()
        if value._doc.class_name != self.class_name or value._doc.definition != self.definition:
            raise ValueError("expected class_name = {} and definition = {}, got class_name= {} and definition = {} instead".format(
                    self.class_name, 
                    self.definition, 
                    value._doc.class_name, 
                    value._doc.definition
                )
            )

    def as_dict(self):
        from .orm import DataStructure
        
        if self.cls is None:
            raise ValueError('Need to pass in a class instance in order to create schema')
        if not issubclass(self.cls, DataStructure):
            raise ValueError('class {} needs to be a subclass of schema.Schema'.format(self.cls.__name__))
        self.cls._initialize_did_doc()
        return {
                'Object': {
                    'definition': self.cls._doc.definition,
                    'class_name': self.cls._doc.class_name, 
                }
            }
    
    def generate_depends_on_statement(self, id):
        return {
            'name': '{}_id'.format(self.cls.__name__),
            'value': id
        }

SUPPORTED_DATATYPE = {
    'Any': Any,
    'String': StringType,
    'Number': NumberType,
    'Integer': IntegerType,
    'List': ListType,
    'Object': ObjectType,
    'Map': DictType
}

