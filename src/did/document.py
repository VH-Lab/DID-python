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

    def add_file(
        self,
        filename,
        location,
        ingest=None,
        delete_original=None,
        location_type=None,
    ):
        """Record a location for one of the document's files.

        Mirrors MATLAB ``did.document/add_file``. Each location carries a
        ``uid``, a ``location_type``, and the ``ingest`` / ``delete_original``
        flags, all defaulted from the location itself: an ``http(s)`` location
        is a ``url`` and defaults to not ingesting and not deleting; anything
        else is a ``file`` and defaults to both.

        The ``uid`` matters beyond bookkeeping. It is how MATLAB finds a file
        it has ingested -- it looks for ``<FileDir>/<uid>`` -- and it is the
        UNIQUE key of the database's ``files`` table, so two locations without
        one collapse into a single row.

        Adding a second location for a file that already has one appends to its
        list, as MATLAB does, rather than replacing it: the shipped
        demoFile.json template carries a local path and a URL for each file.
        """
        if "files" not in self.document_properties:
            self.document_properties["files"] = {"file_info": []}

        files_prop = self.document_properties["files"]
        if "file_info" not in files_prop:
            files_prop["file_info"] = []

        files_prop["file_info"] = self._normalize_file_info(files_prop["file_info"])
        file_info_list = files_prop["file_info"]

        location = str(location).strip()
        detected = (
            "url" if location.lower().startswith(("http://", "https://")) else "file"
        )
        if ingest is None:
            ingest = 0 if detected == "url" else 1
        if delete_original is None:
            delete_original = 0 if detected == "url" else 1
        if location_type is None:
            location_type = detected

        entry = {
            "delete_original": delete_original,
            "uid": ido.IDO.unique_id(),
            "location": location,
            "parameters": "",
            "location_type": location_type,
            "ingest": ingest,
        }

        is_in, info, _ = self.is_in_file_list(filename)
        if is_in and isinstance(info, dict):
            existing = info.get("locations")
            if isinstance(existing, dict):
                existing = [existing]
            elif not isinstance(existing, list):
                existing = []
            existing.append(entry)
            info["locations"] = existing
        else:
            file_info_list.append({"name": filename, "locations": [entry]})

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
                if "base" not in data:
                    data["base"] = {}
                return Document._normalize_to_document_class(data)

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

    def _dependency_index(self, dependency_name):
        """Index of the first depends_on entry named ``dependency_name``.

        Matching is case-insensitive, mirroring MATLAB's ``strcmpi``. Returns
        None when there is no such entry.
        """
        self._ensure_depends_on_list()
        wanted = str(dependency_name).lower()
        for index, dep in enumerate(self.document_properties.get("depends_on", [])):
            if str(dep.get("name", "")).lower() == wanted:
                return index
        return None

    def dependency_value(self, dependency_name, error_if_not_found=True):
        index = self._dependency_index(dependency_name)
        if index is not None:
            return self.document_properties["depends_on"][index].get("value")

        if error_if_not_found:
            raise ValueError(f"Dependency '{dependency_name}' not found.")
        return None

    def set_dependency_value(self, dependency_name, value, error_if_not_found=True):
        index = self._dependency_index(dependency_name)
        if index is not None:
            self.document_properties["depends_on"][index]["value"] = value
            return self

        if error_if_not_found:
            raise ValueError(f"Dependency '{dependency_name}' not found.")

        # Adding a bare `item` to a document that already holds `item_1`,
        # `item_2`, ... is the enumerated-list mistake: the schema declares the
        # un-enumerated stem, so both validators accept the result, but MATLAB's
        # dependency_value_n stops at the first gap and never sees the new
        # entry. Refuse it rather than corrupt the list silently. This is a
        # deliberate divergence -- MATLAB appends without complaint.
        if self._enumerated_count(dependency_name) > 0:
            raise ValueError(
                f"Cannot add a dependency named '{dependency_name}': the "
                f"document already has an enumerated list "
                f"'{dependency_name}_1'... Use add_dependency_value_n"
                f"('{dependency_name}', value) to append to it."
            )

        if "depends_on" not in self.document_properties:
            self.document_properties["depends_on"] = []
        self.document_properties["depends_on"].append(
            {"name": dependency_name, "value": value}
        )
        return self

    # ------------------------------------------------------------------
    # Enumerated dependency lists: `name_1`, `name_2`, ...
    #
    # `n` is the suffix in `name_n`, so it stays 1-based in Python as it is in
    # MATLAB -- it names the entry rather than indexing a list.
    # ------------------------------------------------------------------

    def _enumerated_count(self, dependency_name):
        """How many contiguous ``name_1``, ``name_2``, ... entries exist.

        Counting stops at the first gap, which is what makes the numbering
        matter: MATLAB's dependency_value_n does the same, so an entry above a
        gap is invisible to it.
        """
        count = 0
        while self._dependency_index(f"{dependency_name}_{count + 1}") is not None:
            count += 1
        return count

    def dependency_value_n(self, dependency_name, error_if_not_found=True):
        """Values of the enumerated dependencies ``name_1``, ``name_2``, ...

        Returns the values in order, stopping at the first missing suffix.
        Mirrors MATLAB ``did.document/dependency_value_n``.
        """
        values = []
        index = self._dependency_index(f"{dependency_name}_{len(values) + 1}")
        while index is not None:
            values.append(self.document_properties["depends_on"][index].get("value"))
            index = self._dependency_index(f"{dependency_name}_{len(values) + 1}")

        if not values and error_if_not_found:
            raise ValueError(f"Dependency name {dependency_name} not found.")
        return values

    def add_dependency_value_n(self, dependency_name, value, error_if_not_found=True):
        """Append ``name_(n+1)`` to an enumerated dependency list.

        Mirrors MATLAB ``did.document/add_dependency_value_n``.
        """
        count = self._enumerated_count(dependency_name)
        if "depends_on" not in self.document_properties and error_if_not_found:
            raise ValueError("This document does not have any dependencies.")

        if "depends_on" not in self.document_properties:
            self.document_properties["depends_on"] = []
        self.document_properties["depends_on"].append(
            {"name": f"{dependency_name}_{count + 1}", "value": value}
        )
        return self

    def remove_dependency_value_n(
        self, dependency_name, value, n, error_if_not_found=True
    ):
        """Remove ``name_n`` and renumber the entries above it.

        Renumbering is what keeps the list gap-free, and a gap would truncate
        every later read. Mirrors MATLAB
        ``did.document/remove_dependency_value_n``, including its unused
        ``value`` argument, which is kept for signature parity.
        """
        count = self._enumerated_count(dependency_name)
        if "depends_on" not in self.document_properties and error_if_not_found:
            raise ValueError("This document does not have any dependencies.")

        if n > count and error_if_not_found:
            raise ValueError(
                f"Number to be removed {n} is greater than total number of "
                f"entries {count}."
            )

        index = self._dependency_index(f"{dependency_name}_{n}")
        if index is None:
            raise ValueError(f"Could not locate entry {dependency_name}_{n}")
        del self.document_properties["depends_on"][index]

        for i in range(n + 1, count + 1):
            above = self._dependency_index(f"{dependency_name}_{i}")
            if above is None:
                raise ValueError(f"Could not locate entry {dependency_name}_{i}")
            self.document_properties["depends_on"][above][
                "name"
            ] = f"{dependency_name}_{i - 1}"
        return self
