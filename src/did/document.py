import json
import os
from datetime import datetime, timezone

from . import ido
from .common import PathConstants


def _utc_timestamp():
    """Return the current UTC time as an ISO-8601 millisecond string with 'Z'.

    Ported from NDI-python (ndi.fun.timestamp) with the leap-second guard.
    ``str(datetime.utcnow())`` previously emitted a space-separated,
    timezone-less string ('2026-07-20 22:36:19.611068') that diverges from the
    DID-matlab UTCLeapSeconds ISO-8601 output and from base.schema.json's own
    default ('2018-12-05T18:36:47.241Z'); a JS ``new Date()`` parses the
    tz-less form as LOCAL time. Emit '%Y-%m-%dT%H:%M:%S.%fZ' truncated to
    milliseconds, clamping the (theoretical) leap-second :60 to :59.999.
    """
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    ts = ts.replace(":60.", ":59.999")
    return ts + "Z"


class Document:
    def __init__(self, document_type="base", **kwargs):
        if isinstance(document_type, dict):
            self.document_properties = document_type
        else:
            self.document_properties = self.read_blank_definition(document_type)
            self.document_properties["base"]["id"] = ido.IDO.unique_id()
            self.document_properties["base"]["datestamp"] = _utc_timestamp()

            for key, value in kwargs.items():
                path = key.split(".")
                if len(path) == 1:
                    if key in self.document_properties:
                        self.document_properties[key] = value
                else:
                    d = self.document_properties
                    for p in path[:-1]:
                        existing = d.get(p)
                        if not isinstance(existing, dict):
                            d[p] = {}
                        d = d[p]
                    d[path[-1]] = value

            self._reset_file_info()

    def id(self):
        return self.document_properties.get("base", {}).get("id")

    def set_properties(self, **kwargs):
        for key, value in kwargs.items():
            # This is a simplified way to set properties. A full implementation
            # would need to handle nested properties like 'base.name'.
            path = key.split(".")
            d = self.document_properties
            for p in path[:-1]:
                d = d.setdefault(p, {})
            d[path[-1]] = value
        return self

    def _reset_file_info(self):
        """Clear the file info of a newly created document.

        A class definition may ship template file_info entries (demoFile.json
        does); a new document starts with none of them. MATLAB's
        reset_file_info clears the field unconditionally whenever `files`
        exists -- its emptystruct('name','locations') is an empty struct
        *array*, i.e. an empty list of records, not an empty record.
        """
        if "files" in self.document_properties:
            self.document_properties["files"]["file_info"] = []

    @staticmethod
    def _normalize_file_info(file_info):
        """Normalize file_info to a list.

        MATLAB's jsonencode converts single-element cell arrays to scalars,
        so file_info may arrive as a bare dict instead of a list.
        """
        if isinstance(file_info, dict):
            return [file_info] if file_info else []
        if not isinstance(file_info, list):
            return []
        return file_info

    def is_in_file_list(self, filename):
        file_info = self.document_properties.get("files", {}).get("file_info", [])
        file_info = self._normalize_file_info(file_info)

        for i, info in enumerate(file_info):
            if info.get("name") == filename:
                return True, info, i
        return False, None, None

    def add_file(self, filename, location):
        if "files" not in self.document_properties:
            self.document_properties["files"] = {"file_info": []}

        files_prop = self.document_properties["files"]
        if "file_info" not in files_prop:
            files_prop["file_info"] = []

        files_prop["file_info"] = self._normalize_file_info(files_prop["file_info"])

        file_info_list = files_prop["file_info"]

        is_in, _, _ = self.is_in_file_list(filename)
        if not is_in:
            new_info = {"name": filename, "locations": {"location": location}}
            file_info_list.append(new_info)

    def remove_file(self, filename):
        files_prop = self.document_properties.get("files")
        if files_prop is not None:
            files_prop["file_info"] = self._normalize_file_info(
                files_prop.get("file_info", [])
            )
        is_in, _, index = self.is_in_file_list(filename)
        if is_in:
            del self.document_properties["files"]["file_info"][index]

    @staticmethod
    def set_schema_path(path):
        PathConstants.DEFPATH = path

    @staticmethod
    def read_json_file_location(json_file_location_string):
        """Read the JSON at a document-definition location string.

        Accepts a full path, a ``$PATH``-relative reference such as
        ``$DIDDOCUMENT_EX1/demoA.json``, or a bare class name looked up under
        the configured definition directories. Mirrors MATLAB
        ``did.document.readjsonfilelocation`` (minus the URL case, which
        DID-python has no download path for).
        """
        from .validate import resolve_definition_path

        path = resolve_definition_path(json_file_location_string)
        if path is None:
            return None
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def read_blank_definition(json_file_location_string):
        """Build a blank document from its class definition.

        Reads the definition (``database_documents/<class>.json``), then reads
        each superclass definition recursively and merges it in, so a demoB
        document carries the base, demoA and demoB property lists and the union
        of their dependencies -- and, importantly, carries
        ``document_class.validation``, the pointer add_docs needs to validate
        it. Mirrors MATLAB ``did.document.readblankdefinition``.

        Falls back to reading ``database_schema/<class>.schema.json`` when no
        definition exists, preserving the older DID-python behavior for callers
        that only ship a schema.
        """
        data = Document.read_json_file_location(json_file_location_string)

        if data is not None and "document_class" in data:
            return Document._merge_superclasses(data)

        if data is not None:
            # A flat schema-style file: normalize it the way DID-python used to.
            if "base" not in data:
                data["base"] = {}
            return Document._normalize_to_document_class(data)

        # Legacy path: look for the validation schema directly.
        schema_path = os.path.join(PathConstants.DEFPATH, "database_schema")
        filepath = os.path.join(schema_path, f"{json_file_location_string}.schema.json")
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                data = json.load(f)
                # Ensure the 'base' key is a dict the constructor can stamp
                # base.id / base.datestamp onto. base.schema.json stores 'base'
                # as a LIST of field descriptors (unlike demoA.schema.json,
                # which has no top-level 'base'), so Document('base') /
                # Document() used to raise "TypeError: list indices must be
                # integers". Convert that descriptor list to a {name:
                # default_value} defaults dict. (This is the narrow constructor
                # fix only; the broader build-from-database_documents rework is
                # tracked separately.)
                if "base" not in data:
                    data["base"] = {}
                elif isinstance(data["base"], list):
                    data["base"] = Document._field_descriptors_to_defaults(data["base"])
                # Convert flat classname/superclasses to document_class format
                data = Document._normalize_to_document_class(data)
                return data

        # Fallback for base
        if json_file_location_string == "base":
            return {
                "document_class": {
                    "class_name": "did.document",
                    "property_list_name": "base",
                    "class_version": "1.0",
                    "superclasses": [],
                },
                "base": {"id": "", "name": "", "datestamp": ""},
            }

        raise FileNotFoundError(
            f"Could not find definition for {json_file_location_string}"
        )

    @staticmethod
    def _merge_superclasses(data):
        """Merge each superclass definition into a document definition.

        Superclass lists are unioned by definition string, dependencies by
        name, and every other property list is merged in without overwriting
        what the subclass already defines.
        """
        class_props = data.get("document_class") or {}
        raw_superclasses = class_props.get("superclasses")
        if isinstance(raw_superclasses, dict):
            raw_superclasses = [raw_superclasses]
        if not raw_superclasses:
            return data

        merged_superclasses = []
        for item in raw_superclasses:
            definition = item.get("definition") if isinstance(item, dict) else item
            if not definition:
                continue

            parent = Document.read_json_file_location(definition)
            if parent is None:
                merged_superclasses.append(
                    item if isinstance(item, dict) else {"definition": definition}
                )
                continue
            parent = Document._merge_superclasses(parent)

            entry = dict(item) if isinstance(item, dict) else {"definition": definition}
            parent_class = parent.get("document_class") or {}
            if "property_list_name" in parent_class:
                entry["property_list_name"] = parent_class["property_list_name"]
            if "class_version" in parent_class:
                entry["class_version"] = parent_class["class_version"]
            merged_superclasses.append(entry)

            # The parent's own superclasses join ours.
            for inherited in parent_class.get("superclasses") or []:
                if isinstance(inherited, dict) and "definition" in inherited:
                    merged_superclasses.append(dict(inherited))

            parent = {k: v for k, v in parent.items() if k != "document_class"}

            # Dependencies are unioned by name, subclass entries winning.
            if "depends_on" in data and "depends_on" in parent:
                combined = list(data["depends_on"]) + list(parent.pop("depends_on"))
                seen = set()
                unique = []
                for dependency in combined:
                    name = (
                        dependency.get("name")
                        if isinstance(dependency, dict)
                        else dependency
                    )
                    if name in seen:
                        continue
                    seen.add(name)
                    unique.append(dependency)
                data["depends_on"] = unique

            for key, value in parent.items():
                if key not in data:
                    data[key] = value

        # Unique by definition, preserving order.
        seen = set()
        unique_superclasses = []
        for entry in merged_superclasses:
            definition = entry.get("definition")
            if definition in seen:
                continue
            seen.add(definition)
            unique_superclasses.append(entry)
        class_props["superclasses"] = unique_superclasses
        data["document_class"] = class_props
        return data

    @staticmethod
    def _field_descriptors_to_defaults(descriptors):
        """Convert a schema field-descriptor list to a {name: default_value} dict.

        Each descriptor is a dict with at least a 'name' and (usually) a
        'default_value'. Used to turn base.schema.json's 'base' list into a
        blank base group the constructor can stamp id/datestamp onto.
        """
        defaults = {}
        for field in descriptors:
            if isinstance(field, dict) and "name" in field:
                defaults[field["name"]] = field.get("default_value", "")
        return defaults

    @staticmethod
    def _normalize_to_document_class(data):
        """Convert flat schema format to MATLAB-compatible document_class format."""
        if "document_class" in data:
            return data
        class_name = data.pop("classname", "")
        superclasses = data.pop("superclasses", [])
        data["document_class"] = {
            "class_name": class_name,
            "property_list_name": class_name,
            "class_version": 1,
            "superclasses": superclasses,
        }
        return data

    def _ensure_depends_on_list(self):
        """Normalize depends_on to a list if it is a bare dict."""
        dep = self.document_properties.get("depends_on")
        if isinstance(dep, dict):
            self.document_properties["depends_on"] = [dep]

    def dependency_value(self, dependency_name, error_if_not_found=True):
        self._ensure_depends_on_list()
        if "depends_on" in self.document_properties:
            for dep in self.document_properties["depends_on"]:
                if dep.get("name") == dependency_name:
                    return dep.get("value")

        if error_if_not_found:
            raise ValueError(f"Dependency '{dependency_name}' not found.")
        return None

    def set_dependency_value(self, dependency_name, value, error_if_not_found=True):
        self._ensure_depends_on_list()
        if "depends_on" in self.document_properties:
            for dep in self.document_properties["depends_on"]:
                if dep.get("name") == dependency_name:
                    dep["value"] = value
                    return self

        if error_if_not_found:
            raise ValueError(f"Dependency '{dependency_name}' not found.")

        # If not found and not erroring, add it
        if "depends_on" not in self.document_properties:
            self.document_properties["depends_on"] = []
        self.document_properties["depends_on"].append(
            {"name": dependency_name, "value": value}
        )
        return self

    # ... other methods like validate, plus, etc. would be implemented here ...
