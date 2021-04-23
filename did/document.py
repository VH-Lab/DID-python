import json
import os
import struct
import random
import copy
from .id import DIDId
from .time import current_time


class DIDDocument:
    # constructors for DIDDocument
    def __init__(self, data=None, document_type=None):
        if document_type:
            self.data = self._from_schema(document_type)
            self.data['base']['id'] = DIDId().id
            self.data['base']['datestamp'] = current_time()
        else:
            try:
                data.get('base').get('id')
                self.data = data
            except AttributeError:
                raise Exception('DIDDocuments must be instantiated with a did_document in dict format.')

    def serialize(self):
        return json.dumps(self.data)

    @staticmethod
    def where(document_type):
        from . import settings

        def __search__(path, fname):
            if not os.path.isdir(path):
                raise AttributeError("database document path: {} does not exist".format(path))
            for _dirpath, _dirname, _filename in os.walk(path):
                if os.path.isfile(os.path.join(_dirpath, fname)):
                    return _dirpath, fname
                else:
                    for directory in _dirname:
                        try:
                            return __search__(os.path.join(_dirpath, directory), fname)
                        except FileNotFoundError:
                            continue
                    raise FileNotFoundError('{} cannot found from {}'.format(fname, path))
        
        return __search__(settings.get_documentpath(), document_type)

    @classmethod
    def create_emptydoc(cls, class_name, document_type):
        from .settings import get_variable

        data = {
            "document_class": {
                "definition":						"$DIDDOCUMENTPATH" + get_variable('FILESEP') + document_type + '.json',
                "class_name":						class_name,
                "property_list_name":				document_type[:-5] if document_type.endswith('.json') else document_type,
                "class_version":					1,
                "superclasses":                     []
            },
            "base": {
                "session_id":				        "",
                "id":                               DIDId().id,
                "name":							    "",
                "datestamp":						current_time(),
                "document_version":					1
            },
            document_type[:-5] if document_type.endswith('.json') else document_type: {}
        }
        return cls(data=data)

    def _from_schema(self, document_type, starting_path=None):
        """
        return the text from a json file location string in NDI. It looks for the corresponding
        JSON file in the starting_path as well as those in all subdirectory of the starting_path

        :param document_type: 	the name of the json file
        :param starting_path: 	the path from where the json file will be looked for; by
                                default it is set to DIDDOCUMENTPATH in the .env file of where
                                the script is run
        :return: an instance of DIDDocument
        """
        from . import settings

        #look for documentpath from the config file
        if not starting_path:
            starting_path = settings.get_documentpath()

        def __search__(path, fname):
            if not os.path.isdir(path):
                raise AttributeError("database document path: {} does not exist".format(path))
            for _dirpath, _dirname, _filename in os.walk(path):
                if os.path.isfile(os.path.join(_dirpath, fname)):
                    return _dirpath, fname
                else:
                    for directory in _dirname:
                        try:
                            return __search__(os.path.join(_dirpath, directory), fname)
                        except FileNotFoundError:
                            continue
                    raise FileNotFoundError('{} cannot found from {}'.format(fname, path))

        def __readjsonfromblankfile__(jsonstr):
            properties = json.loads(jsonstr)
            if 'document_class' not in properties:
                raise AttributeError("Invalid Document Schema, it must contains document_class as its field")
            superclasses = []
            if 'superclasses' in properties['document_class']:
                for superclass in properties.get('document_class').get('superclasses'):
                    with open(settings.parse_didpath(superclass['definition'])) as f:
                        superclasses.append(__readjsonfromblankfile__(f.read()))
            for superclass in superclasses:
                for key in superclass:
                    if key != 'document_class' and key not in properties:
                        properties[key] = superclass[key]
            properties['depends_on'] = []
            return properties

        if not document_type.endswith('.json'):
            document_type += '.json'
        dirpath, fname = __search__(starting_path, document_type)
        with open(os.path.join(dirpath, fname)) as f:
            result = __readjsonfromblankfile__(f.read())
        result['base'] = {
            "base": {
                "session_id":				        "",
                "id":                               "",
                "name":							    "",
                "datestamp":						"",
                "document_version":					1
            }
        }
        return result

    # Getters and setters
    @property
    def binary_files(self):
        """ Get binary filenames """
        return self.data.get('binary_files')

    @property
    def id(self):
        """
        A globally unique identifier for the document

        :return: did_id string
        """
        return self.data.get('base').get('id')

    @property
    def session_id(self):
        """
        A globally unique identifier for the experimental session. Once made, this never changes; even if
        version is updated.

        :return: did_id string
        """
        return self.data.get('base').get('session_id')

    @session_id.setter
    def session_id(self, id):
        """
        setter for session_id

        :param id:
        """
        self.data['base']['session_id'] = str(id)

    @property
    def name(self):
        """
        A string name for the user. Does not need to be unique. The id, session_id, and version confer uniqueness.
        (Some subtypes may have conditions for name uniqueness; for example, daq_systems must have a unique name.
        But this is not a database-level requirement.)

        :return: ASCII string
        """
        return self.data.get('base').get('name')

    @name.setter
    def name(self, name):
        """
        setter for session_id

        :param name:
        """
        self.data['base']['name'] = str(name)

    @property
    def datestamp(self):
        """
        Time of document creation or modification (that is, it is updated when version is updated)

        :return: ISO-8601 date string, time zone must be UTC leap seconds
        """
        return self.data.get('base').get('datestamp')

    @property
    def database_version(self):
        """
        This probably needs to be an did_id string to help with merging branches where 2 users have modified the same
        database entry (otherwise, might have two copies of "n+1" that need to be dealt with); did_id strings sort by
        time alphabetically, so the time would be a means of differentiating them

        :return: did_id string
        """
        return self.data.get('base').get('database_version')

    @property
    def class_definition(self):
        """
        JSON_definition_location of the definition for this document

        :return: path like string
        """
        return self.data.get('document_class').get('definition')

    @property
    def class_validation(self):
        """
        JSON_schema_location of the schema validation for this document

        :return: path like string
        """
        return self.data.get('document_class').get('validation')

    @property
    def class_name(self):
        """
        Name of this document class

        :return: string
        """
        return self.data.get('document_class').get('class_name')

    @class_name.setter
    def class_name(self, name):
        """
        setter for class_name

        :param name:
        """
        self.class_name['document_class']['class_name'] = str(name)

    @property
    def property_list_name(self):
        """
        String that describes the Property list that is provided by this class

        :return: string
        """
        return self.data.get('document_class').get('property_list_name')

    @property
    def class_version(self):
        """
        Return the version of the schema

        :return: an integer value
        """
        return self.data.get('document_class').get('class_version')

    @property
    def superclasses(self):
        """
        Return the list of superclasses that the method inherents from

        :return: a list of string
        """
        return self.data.get('document_class').get('superclasses')
    
    @property
    def properties(self, cls_name):
        """
        Return the properties associated with cls_name passed in (can be either the 
        parent class or the child class) 

        :return: key-value pairs of the field and its value
        """
        return self.data.get(cls_name)