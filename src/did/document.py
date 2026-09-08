import contextlib
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

        # A name must be served by exactly one mechanism. Checking here
        # catches a malformed class definition the first time anyone
        # constructs one, rather than at the point where the two mechanisms
        # disagree about membership. Outside the else: a document rebuilt
        # from stored properties gets the same check, which is the only way
        # a deliberately malformed declaration reaches this at all.
        # Mirrors MATLAB's localValidateFileDeclarations.
        _validate_file_declarations(self.document_properties)

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

        Also resets file series' per-instance record. The DECLARATION
        (files.file_series, from the class definition) is left alone: it says
        which names are series, which is a property of the class and not of
        this instance.
        """
        if "files" in self.document_properties:
            self.document_properties["files"]["file_info"] = []
            self.document_properties["files"]["series_info"] = []

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
        """Is ``filename`` a valid file name for this document?

        Returns ``(is_in, info, index)``. ``is_in`` mirrors MATLAB's ``b``:
        True when ``filename`` appears in ``files.file_list`` (matched
        case-insensitively), when it resolves to a ``NAME_#`` entry there
        (``foo.ext_12`` matches ``foo.ext_#``), or when it is a member of a
        declared file series. ``info`` and ``index`` describe the
        corresponding ``file_info`` record if one exists; both are ``None``
        for a declared file that has not been added yet and for a series
        member (whose bytes are answered for by the manifest, not by an
        inline file_info entry).

        Mirrors MATLAB ``did.document/is_in_file_list``, including the
        series fallback added in DID-matlab c80ba33. See DID-python#69.
        """
        files = self.document_properties.get("files")
        if not isinstance(files, dict):
            return False, None, None

        file_info = self._normalize_file_info(files.get("file_info", []))

        info = None
        index = None
        for i, entry in enumerate(file_info):
            if str(entry.get("name", "")).lower() == str(filename).lower():
                info = entry
                index = i
                break

        file_list = files.get("file_list") or []
        if isinstance(file_list, str):
            file_list = [file_list]
        lowered_list = [str(n).lower() for n in file_list]
        lowered_name = str(filename).lower()

        if lowered_name in lowered_list:
            return True, info, index

        # Resolve NAME_<digits> to a NAME_# entry, matching MATLAB's second
        # branch of is_in_file_list. Python parses the trailing part strictly
        # (all-digits), as _validate_file_declarations and series_member_of do;
        # MATLAB tightened its parse to match in DID-matlab#199. See the
        # bridge entry and DID-python#69.
        stem, _, tail = str(filename).rpartition("_")
        if stem and tail.isdigit():
            if f"{stem.lower()}_#" in lowered_list:
                return True, info, index

        # Series fallback (c80ba33): a member NAME_<i> is valid when NAME is
        # a declared series, even though the member carries no file_info of
        # its own. info/index stay None in that case, as MATLAB's
        # fI_index stays empty.
        member_stem, _member_index = self.series_member_of(filename)
        if member_stem:
            return True, info, index

        # A file_info entry accepts the name too. MATLAB never lands here
        # because its add_file rejects a name not in file_list; Python's
        # add_file has never enforced that (a separate deviation, unchanged
        # by this port), so treating an inline entry as valid preserves
        # every existing caller that relies on add_file+is_in_file_list to
        # round-trip a name.
        if info is not None:
            return True, info, index

        return False, info, index

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
        # A series member must not gain an inline file_info entry: the
        # manifest would not know about it, so file_uids would answer for a
        # member that series_members and series_count have never heard of,
        # and which of the two answered would depend on the code path the
        # caller took. Members are added together by add_file_series, which
        # records them in the manifest. Mirrors MATLAB's add_file guard
        # (DID:Document:add_file:isSeriesMember). See DID-python#69.
        series_stem, _index = self.series_member_of(filename)
        if series_stem:
            raise ValueError(
                f'"{filename}" is a member of the file series "{series_stem}". '
                f"Members are added together by add_file_series, which records "
                f"them in the series manifest."
            )

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
        _is_in, _info, index = self.is_in_file_list(filename)
        if index is not None:
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
        from .validate import loads_matlab_json, resolve_definition_path

        path = resolve_definition_path(json_file_location_string)
        if path is None:
            return None
        # loads_matlab_json, not json.load: a definition written by MATLAB may
        # carry the bare Inf / -Inf tokens jsondecode accepts. See its comment.
        with open(path, "r") as f:
            return loads_matlab_json(f.read())

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
            data["base"] = Document._blank_base_group(data.get("base"))
            return Document._normalize_to_document_class(data)

        # Legacy path: look for the validation schema directly.
        schema_path = os.path.join(PathConstants.DEFPATH, "database_schema")
        filepath = os.path.join(schema_path, f"{json_file_location_string}.schema.json")
        if os.path.exists(filepath):
            from .validate import loads_matlab_json

            with open(filepath, "r") as f:
                data = loads_matlab_json(f.read())
                data["base"] = Document._blank_base_group(data.get("base"))
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
    def _blank_base_group(base):
        """Return a ``base`` group the constructor can stamp id/datestamp onto.

        ``__init__`` writes ``document_properties["base"]["id"]``, so ``base``
        has to be a dict by the time a blank definition is returned. A schema
        file may instead store it as a LIST of field descriptors -- that is the
        shape of ``base.schema.json``, where ``base`` is
        ``[{"name": "id", "default_value": ""}, ...]`` (``demoA.schema.json``
        has no top-level ``base`` at all, which is why the list form is easy to
        miss). Reading such a file raised
        ``TypeError: list indices must be integers or slices, not str``.

        Both schema-reading branches of ``read_blank_definition`` route through
        here. DID-python#30 guarded only the ``database_schema`` branch, but a
        corpus whose definition directories point straight at the schema files
        is served by the flat-schema branch above it, so the crash survived.
        """
        if isinstance(base, list):
            return Document._field_descriptors_to_defaults(base)
        if isinstance(base, dict):
            return base
        return {}

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

    # ------------------------------------------------------------------
    # File series API. See DID-matlab issue #173.
    #
    # A file series is a `NAME_#` entry in a document's file_list whose
    # members are indexed by a MANIFEST rather than by one file_info struct
    # each. The manifest is a single binary file belonging to the document,
    # named ``NAME`` in file_list. A series member gets no file_info entry
    # and no files-table row: resolution goes through the manifest, so a
    # 28,000-member pyramid level does not carry 8-11 MB of JSON per read.
    # ------------------------------------------------------------------

    def file_uids(self, name):
        """Uids recorded for a named file, from the in-memory document only.

        Returns a list of uid strings for the file ``name``, in the order the
        locations were added by :meth:`add_file`. Returns ``[]`` if this
        document has no such file.

        This reads only the in-memory document; it runs no query. It is the
        first half of resolving a file without touching the database, the
        second half being :func:`did.file.cached_path_for_uid`, and
        :meth:`did.database.Database.cached_path_for_file` is the two
        together.

        IMPORTANT: this answers about the DOCUMENT YOU HOLD, which is not
        necessarily the document in the database. A file added to a stored
        copy after this object was read is not visible here.

        Mirrors ``did.document/fileUids``.
        """
        files = self.document_properties.get("files")
        if not isinstance(files, dict):
            return []
        file_info = self._normalize_file_info(files.get("file_info", []))
        for info in file_info:
            if str(info.get("name", "")).lower() == str(name).lower():
                locations = info.get("locations")
                if isinstance(locations, dict):
                    locations = [locations]
                elif not isinstance(locations, list):
                    return []
                return [loc.get("uid", "") for loc in locations if loc.get("uid")]
        return []

    def series_names(self):
        """Names of the file series this document's class declares.

        Returns the list from ``files.file_series``, or ``[]`` if this class
        declares none. A declared name IS the series' manifest: it is an
        ordinary file in file_list, and its members are ``NAME_1`` ...
        ``NAME_N``.

        Mirrors ``did.document/seriesNames``.
        """
        files = self.document_properties.get("files")
        if not isinstance(files, dict):
            return []
        names = files.get("file_series")
        if not names:
            return []
        if isinstance(names, str):
            return [names]
        return list(names)

    def is_file_series(self, name):
        """Is ``name`` declared as a file series by this document?

        Case-insensitive, matching :meth:`is_in_file_list`.
        Mirrors ``did.document/isFileSeries``.
        """
        lowered = str(name).lower()
        return any(str(n).lower() == lowered for n in self.series_names())

    def series_member_of(self, name):
        """Is ``name`` a member of one of this document's series?

        Returns ``(stem, index)`` if ``name`` parses as ``STEM_<number>`` and
        ``STEM`` is declared as a file series. Otherwise returns ``("", None)``.

        ``index`` is ONE-BASED, the number as written in the name: the
        member ``NAME_1`` is index 1. It addresses the manifest's slots
        directly (see :func:`did.file.read_series_manifest_uid`).

        The number is parsed exactly as ``NAME_#`` files are parsed in
        :meth:`is_in_file_list`, so a name is a member here on precisely the
        terms that would make it a valid file name there.

        Mirrors ``did.document/seriesMemberOf``.
        """
        if not isinstance(name, str) or not name:
            return "", None
        if "_" not in name:
            return "", None
        stem_part, _, tail = name.rpartition("_")
        if not tail.isdigit():
            return "", None
        try:
            index = int(tail)
        except ValueError:
            return "", None
        if not self.is_file_series(stem_part):
            return "", None
        # Return the DECLARED spelling rather than the caller's.
        for declared in self.series_names():
            if str(declared).lower() == stem_part.lower():
                return str(declared), index
        return stem_part, index

    def is_series_member(self, name):
        """Convenience: True when ``name`` is a member of any declared series."""
        stem, _index = self.series_member_of(name)
        return bool(stem)

    def _series_info_index(self, name):
        """Index of ``name``'s entry in files.series_info, or None if absent."""
        files = self.document_properties.get("files")
        if not isinstance(files, dict):
            return None
        series_info = files.get("series_info")
        if not series_info:
            return None
        if isinstance(series_info, dict):
            series_info = [series_info]
        lowered = str(name).lower()
        for i, entry in enumerate(series_info):
            if str(entry.get("name", "")).lower() == lowered:
                return i
        return None

    def _series_info(self, name):
        """Return the series_info entry for ``name``, or None."""
        index = self._series_info_index(name)
        if index is None:
            return None
        series_info = self.document_properties["files"]["series_info"]
        if isinstance(series_info, dict):
            series_info = [series_info]
        return series_info[index]

    def series_count(self, name):
        """Return ``(n, n_present)``: total slots and how many exist.

        ``n`` is the series' declared count -- the series runs ``NAME_1`` to
        ``NAME_N``. ``n_present`` is how many actually exist; a sparse series
        has fewer, because a member with nothing to store is not written.
        Both are 0 for a declared series that has not been populated.

        Mirrors ``did.document/seriesCount``.
        """
        entry = self._series_info(name)
        if entry is None:
            return 0, 0
        return int(entry.get("count", 0) or 0), int(entry.get("n_present", 0) or 0)

    def series_source_root(self, name):
        """Return the directory a series' members came from, or ``""``.

        Mirrors ``did.document/seriesSourceRoot``.
        """
        entry = self._series_info(name)
        if entry is None:
            return ""
        return str(entry.get("source_root", "") or "")

    def series_ingest_locations(self, name):
        """Return the transient list of member paths pending ingestion.

        A list of dicts with keys ``index``, ``uid``, ``location``,
        ``location_type``, ``ingest``, ``delete_original``, ``parameters``.
        Empty when the series has not been added, or the document has
        already been ingested (:meth:`strip_series_ingest_locations` removes
        the entries before storage).

        Mirrors ``did.document/seriesIngestLocations``.
        """
        entry = self._series_info(name)
        if entry is None:
            return []
        ingest = entry.get("ingest_locations")
        if not ingest:
            return []
        if isinstance(ingest, dict):
            return [ingest]
        return list(ingest)

    def add_file_series(
        self,
        name,
        locations,
        indices=None,
        source_root="",
        record_source_names=True,
        uid_width=33,
        delete_original=None,
        ingest=None,
    ):
        """Add a whole file series to this document at once.

        ``name`` must be declared in this class's ``files.file_series``. The
        members become ``NAME_1`` ... ``NAME_N``; ``NAME`` itself is the
        manifest that records them, and is added as an ordinary file.

        ``locations`` is a list of source paths, one per member being added.

        ``indices`` (default ``None``): one-based member numbers, one per
        entry of ``locations``. ``None`` defaults to ``[1..len(locations)]``
        (dense). Pass them explicitly for a SPARSE series.

        ``source_root`` (default ``""``): the directory the members came
        from. ``""`` derives the longest common directory prefix.

        ``record_source_names`` (default ``True``): record each member's
        path relative to the root, as permanent provenance in the manifest.
        Absolute paths never persist.

        ``uid_width`` (default 33): passed through to
        :func:`did.file.write_series_manifest`.

        ``delete_original`` / ``ingest`` follow :meth:`add_file`'s per-type
        defaults when ``None``.

        Mirrors ``did.document/addFileSeries``.
        """
        import tempfile

        from .file import write_series_manifest

        if not self.is_file_series(name):
            raise ValueError(
                f'"{name}" is not declared as a file series by this document '
                f"class. Add it to files.file_series in the class definition."
            )
        if self._series_info_index(name) is not None:
            raise ValueError(
                f'The series "{name}" has already been added to this '
                f"document. Use remove_file_series first to replace it."
            )

        n_locs = len(locations)
        if indices is None:
            indices = list(range(1, n_locs + 1))
        else:
            indices = list(indices)
        if len(indices) != n_locs:
            raise ValueError(
                f"indices has {len(indices)} entries but locations has {n_locs}."
            )
        for idx in indices:
            if not isinstance(idx, int) or idx < 1:
                raise ValueError(
                    "Member indices must be positive integers (one-based)."
                )
        if len(set(indices)) != len(indices):
            raise ValueError("Member indices must be unique.")

        # Derive the root and the relative names, unless told not to.
        source_names = None
        root = source_root or ""
        if record_source_names:
            if not root:
                root = _longest_common_directory(locations)
            if root:
                source_names = _relative_names(locations, root)
            else:
                # No meaningful common root; record none.
                source_names = None
        else:
            root = ""

        max_index = max(indices) if indices else 0

        # Slot i of these arrays is member i (one-based); the manifest
        # writes them zero-based, which is the only place the two differ.
        uids = [""] * max_index
        rel_names = [""] * max_index
        for i, idx in enumerate(indices):
            uids[idx - 1] = ido.IDO.unique_id()
            if source_names is not None:
                rel_names[idx - 1] = source_names[i]

        # Write the manifest to a temp file; add_file will record it under
        # the series' declared name.
        with tempfile.NamedTemporaryFile(
            prefix="did_manifest_", suffix=".manifest", delete=False
        ) as f:
            manifest_path = f.name
        try:
            if source_names is None:
                write_series_manifest(manifest_path, uids, uid_width=uid_width)
            else:
                write_series_manifest(
                    manifest_path, uids, source_names=rel_names, uid_width=uid_width
                )
            # The manifest is an ORDINARY file under the series' own name.
            self.add_file(name, manifest_path)
        except Exception:
            with contextlib.suppress(OSError):
                os.remove(manifest_path)
            raise

        # Where each member's bytes are right now, so ingestion can find
        # them. Transient: strip_series_ingest_locations removes it before
        # a document's JSON is stored.
        ingest_locations = _series_ingest_records(
            locations, indices, uids, delete_original, ingest
        )
        entry = {
            "name": name,
            "count": max_index,
            "n_present": n_locs,
            "source_root": root,
            "ingest_locations": ingest_locations,
        }
        files = self.document_properties.setdefault("files", {})
        series_info = files.get("series_info") or []
        if isinstance(series_info, dict):
            series_info = [series_info]
        series_info.append(entry)
        files["series_info"] = series_info
        return self

    def remove_file_series(self, name):
        """Drop a file series' record from this document.

        Removes the series' record and its manifest file entry. The
        DECLARATION in ``files.file_series`` is untouched -- that belongs
        to the class, not to this document -- so the series may be added
        again.

        Mirrors ``did.document/removeFileSeries``.
        """
        index = self._series_info_index(name)
        if index is None:
            raise ValueError(
                f'The series "{name}" has not been added to this document.'
            )
        series_info = self.document_properties["files"]["series_info"]
        if isinstance(series_info, dict):
            series_info = [series_info]
        del series_info[index]
        self.document_properties["files"]["series_info"] = series_info

        # Drop the manifest's file_info entry too, if one was recorded.
        files = self.document_properties["files"]
        file_info = self._normalize_file_info(files.get("file_info", []))
        lowered = str(name).lower()
        files["file_info"] = [
            info for info in file_info if str(info.get("name", "")).lower() != lowered
        ]
        return self

    @staticmethod
    def strip_series_ingest_locations(props):
        """Empty every ``files.series_info[i].ingest_locations``.

        Call this on the way to storing or shipping a document's JSON. A
        series' member paths are recorded so ingestion can find the bytes;
        they are of no use afterwards, and a level of a lightsheet pyramid
        has tens of thousands of them.

        The field is EMPTIED rather than removed, mirroring MATLAB: rmfield
        would leave a stored document's series_info with one fewer field
        than a fresh one, and adding a series to such a document would then
        fail because the shapes disagree.

        Safe to call on a document with no series, and safe to call twice.
        Mirrors ``did.document.stripSeriesIngestLocations``.
        """
        if not isinstance(props, dict):
            return props
        files = props.get("files")
        if not isinstance(files, dict):
            return props
        series_info = files.get("series_info")
        if not series_info:
            return props
        if isinstance(series_info, dict):
            series_info = [series_info]
        for entry in series_info:
            if isinstance(entry, dict) and "ingest_locations" in entry:
                entry["ingest_locations"] = []
        files["series_info"] = series_info
        return props

    def __eq__(self, other):
        """Two documents are equal iff their ids are equal.

        Mirrors MATLAB ``did.document/eq``, which since DID-matlab commit
        5d0b5d0 compares the id returned by :meth:`id`. Prior to that, the
        MATLAB implementation read a non-existent field and raised, so the
        Python bridge previously marked eq "not portable"; the fix makes it
        portable in both directions.
        """
        if not isinstance(other, Document):
            return NotImplemented
        return self.id() == other.id()

    def __hash__(self):
        # Consistent with __eq__: equal documents hash equal.
        return hash(self.id())


# ---------------------------------------------------------------------------
# File series helpers (module-level so :meth:`Document.add_file_series` stays
# readable). See DID-matlab document.m for the reference implementation.
# ---------------------------------------------------------------------------


def _validate_file_declarations(props):
    """Enforce that a file name is served by ONE mechanism, not two.

    If a name were reachable through both the file-entry path (``file_list``
    plus ``file_info``, with ``NAME_#`` members found by probing) and the
    series path (a manifest that states membership), the two would give
    different answers for it -- and the file-entry path's answer stops at the
    first gap, which is exactly the case series exist to serve. So this is
    not hygiene; it is what makes the two mechanisms safe to coexist.

    Comparisons are case-insensitive, because :meth:`Document.is_file_series`
    and :meth:`Document.series_member_of` match that way and a difference
    they cannot see is not a difference.

    Raises ``ValueError``. MATLAB raises four identified errors here
    (``DID:Document:fileDeclarations:duplicateSeries`` and friends); Python's
    document layer uses plain ValueError throughout, as ``add_file_series``
    already did, so the identifiers live in the messages' wording rather than
    in a field.

    Mirrors MATLAB ``localValidateFileDeclarations``. See DID-python#69.
    """
    if not isinstance(props, dict):
        return
    files = props.get("files")
    if not isinstance(files, dict):
        return
    series_names = files.get("file_series")
    if not series_names:
        return
    if isinstance(series_names, str):
        series_names = [series_names]
    series_names = [str(n) for n in series_names]

    file_list = files.get("file_list") or []
    if isinstance(file_list, str):
        file_list = [file_list]
    file_list = [str(n) for n in file_list]
    lowered_list = [n.lower() for n in file_list]

    for this_series in series_names:
        lowered = this_series.lower()

        if sum(1 for n in series_names if n.lower() == lowered) > 1:
            raise ValueError(
                f'The file series "{this_series}" is declared more than once '
                f"(matching is case-insensitive)."
            )

        # The series name IS its manifest, so it must be an ordinary file.
        if lowered not in lowered_list:
            raise ValueError(
                f'The file series "{this_series}" is not in file_list. A '
                f"series name is its manifest, which is an ordinary file, so "
                f"it must be declared there too."
            )

        # Collision 1: the same family declared both ways.
        if f"{lowered}_#" in lowered_list:
            raise ValueError(
                f'"{this_series}" is declared as a file series and '
                f'"{this_series}_#" is also in file_list. '
                f'"{this_series}_12" would match both, and the two '
                f"mechanisms would disagree about membership."
            )

    # Collision 2: a literal entry that a series would shadow. A trailing
    # integer is resolved before the literal name, so such an entry is
    # unreachable.
    lowered_series = {n.lower() for n in series_names}
    for this_name in file_list:
        if not this_name or this_name.endswith("#"):
            continue
        stem, _, tail = this_name.rpartition("_")
        if not stem or not tail.isdigit():
            continue
        if stem.lower() in lowered_series:
            raise ValueError(
                f'file_list entry "{this_name}" is unreachable: it parses as '
                f'member {tail} of the file series "{stem}", so the series '
                f"answers for it."
            )


def _longest_common_directory(locations):
    """Longest common DIRECTORY prefix of ``locations``, or ``""``.

    ``""`` is returned for a trivial result -- the filesystem root, or
    paths with nothing in common -- because the alternative is recording
    absolute paths in the manifest, which discloses a directory layout.
    A URL is likewise given no root: it carries no home directory to leak,
    and is stored whole. Mirrors MATLAB ``localCommonRoot``.
    """
    if not locations:
        return ""
    parents = []
    for loc in locations:
        text = str(loc)
        if not text:
            return ""
        if "://" in text:
            return ""
        parents.append(os.path.dirname(text))
    common = parents[0]
    for parent in parents[1:]:
        common = _common_prefix_dir(common, parent)
        if not common:
            return ""
    if not common or common in (os.sep, "."):
        return ""
    return common


def _common_prefix_dir(a, b):
    """Common leading path components of A and B, joined."""
    import re

    parts_a = [p for p in re.split(r"[\\/]", a) if p or a.startswith(("/", "\\"))]
    parts_b = [p for p in re.split(r"[\\/]", b) if p or b.startswith(("/", "\\"))]
    # Preserve a leading empty (POSIX root) as an empty first segment.
    if a.startswith(("/", "\\")):
        parts_a = [""] + [p for p in parts_a if p]
    if b.startswith(("/", "\\")):
        parts_b = [""] + [p for p in parts_b if p]
    n = min(len(parts_a), len(parts_b))
    k = 0
    for i in range(n):
        if parts_a[i] == parts_b[i]:
            k = i + 1
        else:
            break
    if k == 0:
        return ""
    if k == 1 and parts_a[0] == "":
        return os.sep
    joined = os.sep.join(parts_a[:k])
    if parts_a[0] == "" and not joined.startswith(os.sep):
        joined = os.sep + joined
    return joined


def _relative_names(locations, root):
    """Each location's path RELATIVE to ROOT, forward-slash separated.

    Uses ``/`` as the separator so a manifest written on one platform reads
    the same on another. Raises ``ValueError`` if any location does not
    live under ``root`` -- recording it would store an absolute path.
    Mirrors MATLAB ``localRelativeNames``.
    """
    import re

    root_parts = [p for p in re.split(r"[\\/]", root) if p]
    if root.startswith(("/", "\\")):
        root_parts = [""] + root_parts
    out = []
    for loc in locations:
        parts = [p for p in re.split(r"[\\/]", str(loc)) if p]
        if str(loc).startswith(("/", "\\")):
            parts = [""] + parts
        if len(parts) <= len(root_parts) or parts[: len(root_parts)] != root_parts:
            raise ValueError(
                f'Location "{loc}" is not under the series source root '
                f'"{root}". Recording it would store an absolute path.'
            )
        out.append("/".join(parts[len(root_parts) :]))
    return out


def _series_ingest_records(locations, indices, uids, delete_original, ingest_option):
    """Build the transient uid -> source-path record for a series' members.

    Shaped like a file_info location so the ingestion loop can treat the two
    the same way, plus ``index`` so the member's files-table filename
    (``NAME_<index>``) is known without consulting the manifest.

    Defaults follow :meth:`Document.add_file`: a URL is not ingested and its
    original is not deleted; a local file is ingested and, unless the caller
    says otherwise, its original is deleted. ``None`` for either override
    uses the per-type default; a value overrides it for every member.
    Mirrors MATLAB ``localIngestLocations``.
    """
    entries = []
    for i, raw_location in enumerate(locations):
        loc = str(raw_location).strip()
        if loc.lower().startswith(("http://", "https://")):
            location_type = "url"
            default_ingest = 0
            default_delete = 0
        else:
            location_type = "file"
            default_ingest = 1
            default_delete = 1

        this_delete = default_delete if delete_original is None else delete_original
        this_ingest = default_ingest if ingest_option is None else ingest_option

        entries.append(
            {
                "index": indices[i],
                "uid": uids[indices[i] - 1],
                "location": loc,
                "location_type": location_type,
                "ingest": this_ingest,
                "delete_original": this_delete,
                "parameters": "",
            }
        )
    return entries
