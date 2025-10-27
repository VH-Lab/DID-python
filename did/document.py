import datetime
import uuid
import json
import os

class Document:
    def __init__(self, document_type, **options):
        made_from_struct = False
        if isinstance(document_type, dict):
            self.document_properties = document_type
            made_from_struct = True
        else:
            self.document_properties = self._read_blank_definition(document_type)
            self.document_properties['base']['id'] = str(uuid.uuid4())
            self.document_properties['base']['datestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            for key, value in options.items():
                # This is where the original code used eval, which is unsafe.
                # We will set properties by traversing the dictionary.
                keys = key.split('.')
                d = self.document_properties
                for k in keys[:-1]:
                    d = d.setdefault(k, {})
                d[keys[-1]] = value

        if not made_from_struct:
            self._reset_file_info()

    def _reset_file_info(self):
        if 'files' in self.document_properties:
            self.document_properties['files']['file_info'] = []

    def id(self):
        return self.document_properties.get('base', {}).get('id')

    def _read_blank_definition(self, document_type):
        path = self._read_json_file_location(document_type)
        with open(path) as f:
            data = json.load(f)

        if 'document_class' in data and 'superclasses' in data['document_class']:
            for superclass in data['document_class']['superclasses']:
                superclass_data = self._read_blank_definition(superclass['definition'])
                data = {**superclass_data, **data}

        return data

    def _read_json_file_location(self, json_file_location_string):
        from .common.path_constants import PathConstants
        for key, value in PathConstants.DEFINITIONS.items():
            if key in json_file_location_string:
                return json_file_location_string.replace(key, value)

        # Fallback for filenames without placeholders
        if not json_file_location_string.endswith('.json'):
            json_file_location_string += '.json'

        for path in PathConstants.DEFINITIONS.values():
            full_path = os.path.join(path, json_file_location_string)
            if os.path.exists(full_path):
                return full_path

        raise FileNotFoundError(f"Could not find a match for {json_file_location_string}")

    def add_dependency(self, name, value):
        if 'depends_on' not in self.document_properties:
            self.document_properties['depends_on'] = []
        self.document_properties['depends_on'].append({'name': name, 'value': value})

    def get_dependency_value(self, dependency_name, error_if_not_found=True):
        if 'depends_on' in self.document_properties:
            for dep in self.document_properties['depends_on']:
                if dep['name'] == dependency_name:
                    return dep['value']
        if error_if_not_found:
            raise ValueError(f"Dependency '{dependency_name}' not found.")
        return None

    def set_dependency_value(self, dependency_name, value, error_if_not_found=True):
        if 'depends_on' in self.document_properties:
            for dep in self.document_properties['depends_on']:
                if dep['name'] == dependency_name:
                    dep['value'] = value
                    return
        if error_if_not_found:
            raise ValueError(f"Dependency '{dependency_name}' not found.")

    def add_dependency_value_n(self, dependency_name, value):
        if 'depends_on' not in self.document_properties:
            self.document_properties['depends_on'] = []

        i = 1
        while True:
            new_dependency_name = f"{dependency_name}_{i}"
            if not any(dep['name'] == new_dependency_name for dep in self.document_properties['depends_on']):
                self.add_dependency(new_dependency_name, value)
                return
            i += 1

    def remove_dependency_value_n(self, dependency_name, n, error_if_not_found=True):
        dependency_to_remove = f"{dependency_name}_{n}"
        if 'depends_on' in self.document_properties:
            self.document_properties['depends_on'] = [
                dep for dep in self.document_properties['depends_on'] if dep['name'] != dependency_to_remove
            ]
        elif error_if_not_found:
            raise ValueError("This document does not have any dependencies.")

        i = n + 1
        while True:
            old_dependency_name = f"{dependency_name}_{i}"
            new_dependency_name = f"{dependency_name}_{i-1}"

            found = False
            for dep in self.document_properties.get('depends_on', []):
                if dep['name'] == old_dependency_name:
                    dep['name'] = new_dependency_name
                    found = True
                    break

            if not found:
                break
            i += 1

    def add_file(self, name, location, ingest=None, delete_original=None, location_type=None):
        if 'files' not in self.document_properties:
            raise ValueError("This document type does not accept files.")

        name = name.strip()
        location = location.strip()

        detected_location_type = 'file'
        if location.lower().startswith(('http://', 'https://')):
            detected_location_type = 'url'

        if ingest is None:
            ingest = 1 if detected_location_type == 'file' else 0
        if delete_original is None:
            delete_original = 1 if detected_location_type == 'file' else 0
        if location_type is None:
            location_type = detected_location_type

        location_here = {
            'delete_original': delete_original,
            'uid': str(uuid.uuid4()),
            'location': location,
            'parameters': '',
            'location_type': location_type,
            'ingest': ingest,
        }

        if 'file_info' not in self.document_properties['files']:
            self.document_properties['files']['file_info'] = []

        for file_info in self.document_properties['files']['file_info']:
            if file_info['name'] == name:
                file_info['locations'].append(location_here)
                return

        self.document_properties['files']['file_info'].append({
            'name': name,
            'locations': [location_here]
        })

    def remove_file(self, name, location=None, error_if_no_file_info=False):
        if 'files' not in self.document_properties or 'file_info' not in self.document_properties['files']:
            if error_if_no_file_info:
                raise ValueError(f"No file_info for name '{name}'.")
            return

        file_info_index = -1
        for i, file_info in enumerate(self.document_properties['files']['file_info']):
            if file_info['name'] == name:
                file_info_index = i
                break

        if file_info_index == -1:
            if error_if_no_file_info:
                raise ValueError(f"No file_info for name '{name}'.")
            return

        if location is None:
            del self.document_properties['files']['file_info'][file_info_index]
            return

        locations = self.document_properties['files']['file_info'][file_info_index]['locations']

        location_match_index = -1
        for i, loc in enumerate(locations):
            if loc['location'] == location:
                location_match_index = i
                break

        if location_match_index == -1:
            if error_if_no_file_info:
                raise ValueError(f"No match found for file '{name}' with location '{location}'.")
        else:
            del locations[location_match_index]

    def is_in_file_list(self, name):
        if 'files' not in self.document_properties or 'file_list' not in self.document_properties['files']:
            return False, "This type of document does not accept files; it has no 'files' field.", None

        search_name = name
        if '_' in name:
            parts = name.rsplit('_', 1)
            if parts[1].isdigit():
                search_name = f"{parts[0]}_#"

        if search_name not in self.document_properties['files']['file_list']:
            return False, f"No such file {name} in file_list of did.document; file must match an expected name.", None

        for i, file_info in enumerate(self.document_properties['files'].get('file_info', [])):
            if file_info['name'] == name:
                return True, "", i

        return True, "", None

    def __eq__(self, other):
        if not isinstance(other, Document):
            return NotImplemented
        return self.id() == other.id()
