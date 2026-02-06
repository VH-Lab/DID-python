import json
import os
from datetime import datetime
from . import datastructures
from . import ido
from .common import PathConstants

class Document:
    def __init__(self, document_type='base', **kwargs):
        if isinstance(document_type, dict):
            self.document_properties = document_type
        else:
            self.document_properties = self.read_blank_definition(document_type)
            self.document_properties['base']['id'] = ido.IDO.unique_id()
            self.document_properties['base']['datestamp'] = str(datetime.utcnow())

            for key, value in kwargs.items():
                # This is a simplified way to set properties. A full implementation
                # would need to handle nested properties like 'base.name'.
                if key in self.document_properties:
                    self.document_properties[key] = value

            self._reset_file_info()

    def id(self):
        return self.document_properties.get('base', {}).get('id')

    def set_properties(self, **kwargs):
        for key, value in kwargs.items():
            # This is a simplified way to set properties. A full implementation
            # would need to handle nested properties like 'base.name'.
            path = key.split('.')
            d = self.document_properties
            for p in path[:-1]:
                d = d.setdefault(p, {})
            d[path[-1]] = value
        return self

    def _reset_file_info(self):
        if 'files' in self.document_properties:
            # Only reset if file_info is missing or we are initializing a new document
            if 'file_info' not in self.document_properties['files']:
                self.document_properties['files']['file_info'] = datastructures.empty_struct('name', 'locations')

    def is_in_file_list(self, filename):
        file_info = self.document_properties.get('files', {}).get('file_info', [])
        if isinstance(file_info, dict) and not file_info:
             file_info = []

        for i, info in enumerate(file_info):
            if info.get('name') == filename:
                return True, info, i
        return False, None, None

    def add_file(self, filename, location):
        if 'files' not in self.document_properties:
            self.document_properties['files'] = {'file_info': []}

        files_prop = self.document_properties['files']
        if 'file_info' not in files_prop:
             files_prop['file_info'] = []

        if isinstance(files_prop['file_info'], dict) and not files_prop['file_info']:
            files_prop['file_info'] = []

        file_info_list = files_prop['file_info']

        is_in, _, _ = self.is_in_file_list(filename)
        if not is_in:
             new_info = {
                 'name': filename,
                 'locations': {'location': location}
             }
             file_info_list.append(new_info)

    def remove_file(self, filename):
        is_in, _, index = self.is_in_file_list(filename)
        if is_in:
            del self.document_properties['files']['file_info'][index]

    @staticmethod
    def set_schema_path(path):
        PathConstants.DEFPATH = path

    @staticmethod
    def read_blank_definition(json_file_location_string):
        # This is a simplified version of the Matlab function.
        # It reads a JSON file from a predefined location.

        schema_path = os.path.join(PathConstants.DEFPATH, 'database_schema')
        filepath = os.path.join(schema_path, f"{json_file_location_string}.schema.json")
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
                # Ensure the 'base' key exists
                if 'base' not in data:
                    data['base'] = {}
                return data

        # Fallback for base
        if json_file_location_string == 'base':
            return {
                "document_class": {
                    "class_name": "did.document",
                    "property_list_name": "base",
                    "class_version": "1.0",
                    "superclasses": []
                },
                "base": {
                    "id": "",
                    "name": "",
                    "datestamp": ""
                }
            }

        raise FileNotFoundError(f"Could not find definition for {json_file_location_string}")

    def dependency_value(self, dependency_name, error_if_not_found=True):
        if 'depends_on' in self.document_properties:
            for dep in self.document_properties['depends_on']:
                if dep.get('name') == dependency_name:
                    return dep.get('value')

        if error_if_not_found:
            raise ValueError(f"Dependency '{dependency_name}' not found.")
        return None

    def set_dependency_value(self, dependency_name, value, error_if_not_found=True):
        if 'depends_on' in self.document_properties:
            for dep in self.document_properties['depends_on']:
                if dep.get('name') == dependency_name:
                    dep['value'] = value
                    return self

        if error_if_not_found:
            raise ValueError(f"Dependency '{dependency_name}' not found.")

        # If not found and not erroring, add it
        if 'depends_on' not in self.document_properties:
            self.document_properties['depends_on'] = []
        self.document_properties['depends_on'].append({'name': dependency_name, 'value': value})
        return self

    # ... other methods like validate, plus, etc. would be implemented here ...