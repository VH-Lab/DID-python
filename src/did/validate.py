"""Document-vs-schema validation.

This is the Python counterpart of the validation MATLAB performs in
``did.database``: ``validate_docs``, ``get_document_schema``,
``validate_doc_vs_schema``, ``validate_field_type_and_value`` and the
``checkfiles`` / ``isfilenamematch`` / ``canfindonefile`` static helpers.

The live entry point in both languages is ``database.add_docs``, which
validates by default. (MATLAB's ``did.document.validate`` is *not* the entry
point -- it calls ``did.validate``, which does not exist in DID-matlab, so that
method raises if called. It is not mirrored here.)

Error identifiers mirror MATLAB's (``DID:Database:ValidationDependsOn`` and
friends) and are carried on :class:`ValidationError.identifier` so callers can
branch on the same strings in either language.
"""

import math
import os
import re
import warnings
from datetime import datetime

from .common import PathConstants

# MATLAB's validate_doc_vs_schema sets this constant; kept here under the same
# name so the two implementations stay diffable.
IGNORE_DID_CLASS_PREFIX = True

# Environment variable that opts in to the missing-optional-dependency warning.
# Off by default so it does not surface in normal releases; see
# MissingOptionalDependencyWarning.
FORCE_WARNINGS_ENV_VAR = "DID_FORCE_VALIDATION_WARNINGS"


class ValidationError(ValueError):
    """A document failed schema validation.

    ``identifier`` is the MATLAB error identifier for the same failure, e.g.
    ``'DID:Database:ValidationDependsOn'``.
    """

    def __init__(self, identifier, message):
        super().__init__(message)
        self.identifier = identifier


class MissingOptionalDependencyWarning(UserWarning):
    """A schema-declared optional dependency is absent from a document.

    Not an error: a ``depends_on`` entry whose schema marks it
    ``mustbenotempty`` false/empty may be omitted. The warning exists to gauge
    how often that happens, and is opt-in -- it is only emitted when
    ``DID_FORCE_VALIDATION_WARNINGS`` is set to a non-zero value.
    """


def _raise(identifier, message):
    raise ValidationError(identifier, message)


def _assert(condition, identifier, message):
    if not condition:
        _raise(identifier, message)


def forced_warnings_enabled():
    """True when DID_FORCE_VALIDATION_WARNINGS opts in to validation warnings.

    Mirrors MATLAB: unset or empty is off, '0' is off, any other value --
    including a non-numeric one -- is on.
    """
    env_value = (os.environ.get(FORCE_WARNINGS_ENV_VAR) or "").strip()
    if not env_value:
        return False
    try:
        return float(env_value) != 0
    except ValueError:
        return True


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------


def resolve_definition_path(location_string):
    """Resolve a ``$PATH``-style JSON location to a filesystem path.

    Handles the ``$DIDDOCUMENT_EX1/x.json`` form used by document definitions
    and validation schemas, a plain absolute path, and a bare name looked up
    under the configured definition directories. Returns None if nothing
    matches. Mirrors the path-resolution half of MATLAB's
    ``did.document.readjsonfilelocation`` and ``database.get_document_schema``.
    """
    if not location_string:
        return None

    location_string = str(location_string).replace("\\", "/")

    # An already-usable path.
    if os.path.isfile(location_string):
        return location_string

    definitions = PathConstants.DEFINITIONS

    candidates = []
    placeholders = re.findall(r"\$\w+", location_string)
    if len(placeholders) > 1:
        _raise(
            "DID:Document:readjsonfilelocation",
            f"More than one $PATH indicated in '{location_string}'",
        )
    if placeholders:
        placeholder = placeholders[0]
        locations = definitions.get(placeholder)
        if locations is None:
            return None
        if not isinstance(locations, (list, tuple)):
            locations = [locations]
        for location in locations:
            candidates.append(
                location_string.replace(placeholder, str(location).replace("\\", "/"))
            )
    else:
        # A bare name: search every definition directory (and its subdirectories).
        name = location_string
        if not name.endswith(".json"):
            name = name + ".json"
        for locations in definitions.values():
            if not isinstance(locations, (list, tuple)):
                locations = [locations]
            for location in locations:
                candidates.append(os.path.join(str(location), name))
                for root, _dirs, _files in os.walk(str(location)):
                    candidates.append(os.path.join(root, name))

    # MATLAB tries the literal name, then '.schema.json', then '_schema.json'.
    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate
        schema_candidate = re.sub(r"\.json$", ".schema.json", candidate)
        if os.path.isfile(schema_candidate):
            return schema_candidate
        underscore_candidate = schema_candidate.replace(".schema.json", "_schema.json")
        if os.path.isfile(underscore_candidate):
            return underscore_candidate

    return None


# MATLAB's jsondecode accepts the bare tokens ``Inf``, ``-Inf`` and ``NaN``
# where JSON proper has no literal for them, and definition files in the wild
# use them: NDI-matlab writes ``"parameters": [-Inf,Inf,0]`` for a double field
# with no bounds, and NDIcalc-vis-matlab writes ``[NaN,NaN]``. Python's json
# accepts ``Infinity``/``-Infinity``/``NaN`` but not ``Inf``, so every such file
# was unreadable here -- ``DID:Database:ValidationFileBad`` -- while MATLAB read
# it and validated against it happily.
#
# Widening ``Inf`` to ``Infinity`` outside of strings is enough to close the
# gap. The alternative, rewriting the shared JSON to a finite bound, would
# change what those schemas mean in the language that can already read them.
#
# ``\bInf\b`` cannot match inside ``Infinity`` (the trailing boundary fails
# against ``i``) or inside a word like ``Info``, and a preceding ``-`` is left
# alone, so ``-Inf`` becomes ``-Infinity``. The string alternative comes first
# so that ``Inf`` inside a documentation string is matched as part of that
# string and returned untouched.
_MATLAB_INF = re.compile(r'"(?:[^"\\]|\\.)*"|\bInf\b')


def _widen_matlab_inf(match):
    text = match.group(0)
    return text if text.startswith('"') else "Infinity"


def loads_matlab_json(text):
    """``json.loads``, but tolerating MATLAB's bare ``Inf`` / ``-Inf``.

    ``NaN`` needs no help: Python's json already accepts it. Raises
    ``ValueError`` like ``json.loads`` if the text is not JSON for any other
    reason.
    """
    import json

    try:
        return json.loads(text)
    except ValueError:
        return json.loads(_MATLAB_INF.sub(_widen_matlab_inf, text))


def get_document_schema(schema_filename):
    """Read and parse the JSON at a ``$PATH``-style location.

    Mirrors MATLAB ``database.get_document_schema``: used both for validation
    schemas and (during superclass recursion) for document definitions.
    """

    path = resolve_definition_path(schema_filename)
    if path is None:
        _raise(
            "DID:Database:ValidationFileMissing",
            f'Validation file "{schema_filename}" not found',
        )

    try:
        with open(path, "r") as handle:
            text = handle.read()
    except OSError:
        _raise(
            "DID:Database:ValidationFileCorrupt",
            f'Validation file "{schema_filename}" cannot be read',
        )

    try:
        return loads_matlab_json(text)
    except ValueError:
        _raise(
            "DID:Database:ValidationFileBad",
            f'Validation file "{schema_filename}" has invalid JSON format',
        )


# ---------------------------------------------------------------------------
# Small structural helpers
# ---------------------------------------------------------------------------


def _as_list(value):
    """Normalize to a list.

    MATLAB's jsonencode unwraps single-element cell arrays into scalars, so a
    field that should be a list can arrive as a bare dict or string.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _is_empty(value):
    if value is None:
        return True
    if isinstance(value, str):
        return value == ""
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return value.size == 0
    except ImportError:
        pass
    return False


def _is_truthy(value):
    """MATLAB's ``~isempty(v) && v`` test for a mustbenotempty flag."""
    if _is_empty(value):
        return False
    if isinstance(value, str):
        return value.lower() not in ("0", "false", "no")
    return bool(value)


def _strip_did_prefix(name):
    return re.sub(r"did\.", "", str(name), flags=re.IGNORECASE)


def _basename_no_ext(definition):
    """'$DIDDOCUMENT_EX1/base.json' -> 'base' (MATLAB's fileparts)."""
    name = re.sub(r".*[/\\]", "", str(definition))
    return re.sub(r"\.[^.]*$", "", name)


def _superclass_names(class_props):
    """Bare, unique, sorted superclass names from a document's document_class."""
    names = []
    for item in _as_list(class_props.get("superclasses")):
        if isinstance(item, str):
            names.append(_basename_no_ext(item))
        elif isinstance(item, dict) and "definition" in item:
            names.append(_basename_no_ext(item["definition"]))
    return sorted(set(names))


def _superclass_definitions(class_props):
    """The raw definition strings for a document's superclasses, in order."""
    definitions = []
    for item in _as_list(class_props.get("superclasses")):
        if isinstance(item, str):
            definitions.append(item)
        elif isinstance(item, dict) and "definition" in item:
            definitions.append(item["definition"])
    return definitions


def _strip_enumeration_suffix(name):
    """'item_3' -> 'item'.

    Dependency names may be enumerated with a trailing ``_<n>``; the schema
    declares the un-enumerated stem. Mirrors MATLAB's ``regexp(name,'_(\\d*)\\>')``,
    which truncates at the first such match.
    """
    match = re.search(r"_(\d*)\b", str(name))
    if match is None:
        return str(name)
    return str(name)[: match.start()]


def _doc_dependency_entries(doc_props):
    """The document's depends_on entries as a list of dicts."""
    return [d for d in _as_list(doc_props.get("depends_on")) if isinstance(d, dict)]


# ---------------------------------------------------------------------------
# Field type and value validation
# ---------------------------------------------------------------------------


def _numeric_parameters(definition):
    params = definition.get("parameters", [])
    if _is_empty(params):
        return []
    if not isinstance(params, (list, tuple)):
        return [params]
    return list(params)


def _is_numeric(value):
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return True
    try:
        import numpy as np

        if isinstance(value, (np.integer, np.floating, np.bool_)):
            return True
    except ImportError:
        pass
    return False


def _is_nan(value):
    try:
        return isinstance(value, float) and math.isnan(value)
    except TypeError:
        return False


def validate_field_type_and_value(doc_name, field_name, value, definition):
    """Validate one field value against its schema definition.

    Mirrors MATLAB ``database.validate_field_type_and_value``.
    """
    expected_type = str(definition.get("type", "")).lower()
    params = _numeric_parameters(definition)

    if expected_type in ("integer", "double"):
        identifier = (
            "DID:Database:ValidationFieldInteger"
            if expected_type == "integer"
            else "DID:Database:ValidationFieldDouble"
        )
        label = "Integer" if expected_type == "integer" else "Double"

        _assert(
            _is_empty(value) or _is_numeric(value),
            identifier,
            f"Invalid non-numeric sub-field {field_name} found in {doc_name}",
        )
        _assert(
            len(params) in (3, 4),
            identifier,
            f"3 or 4 parameters must be defined for {label} fields in a document "
            f"schema, but {len(params)} defined",
        )

        can_be_empty = _is_truthy(params[3]) if len(params) >= 4 else False
        if _is_empty(value):
            is_ok = can_be_empty
        elif _is_nan(value):
            is_ok = _is_truthy(params[2])
        else:
            is_ok = params[0] <= value <= params[1]
        _assert(
            is_ok,
            identifier,
            f"Invalid sub-field {field_name} value found in {doc_name}",
        )

        if expected_type == "integer" and not _is_empty(value) and not _is_nan(value):
            numeric = float(value)
            _assert(
                abs(numeric - math.trunc(numeric)) < 1e-12,
                identifier,
                f"Invalid non-integer value {numeric} provided",
            )

    elif expected_type == "matrix":
        identifier = "DID:Database:ValidationFieldMatrix"
        shape = _matrix_shape(value)
        _assert(
            _is_empty(value) or shape is not None,
            identifier,
            f"Invalid non-numeric Matrix sub-field {field_name} found in {doc_name}",
        )
        _assert(
            len(params) >= 2,
            identifier,
            f"At least 2 parameters must be defined for Matrix fields in a "
            f"document schema, but {len(params)} defined",
        )
        if shape is None:
            shape = (0, 0)
        non_nan = [i for i, p in enumerate(params) if not _is_nan_param(p)]
        if not non_nan:
            is_ok = True
        elif any(params[i] == 1 for i in non_nan):
            # A vector is a vector: allow a row/column switch. Temporary in
            # MATLAB too -- kept identical so the two agree.
            is_ok = any(dimension == 1 for dimension in shape)
        else:
            is_ok = all(i < len(shape) and shape[i] == params[i] for i in non_nan)
        rows = shape[0] if len(shape) > 0 else 0
        columns = shape[1] if len(shape) > 1 else 0
        _assert(
            is_ok,
            identifier,
            f"Invalid sub-field {field_name} size {rows}x{columns} found in {doc_name}",
        )

    elif expected_type == "timestamp":
        identifier = "DID:Database:ValidationFieldTimestamp"
        _assert(
            isinstance(value, str),
            identifier,
            f"Invalid non-timestamp sub-field {field_name} found in {doc_name}",
        )
        _assert(
            _parses_as_timestamp(value),
            identifier,
            f"Invalid timestamp sub-field {field_name} found in {doc_name}",
        )

    elif expected_type in ("char", "string"):
        identifier = "DID:Database:ValidationFieldChar"
        _assert(
            _is_empty(value) or isinstance(value, str),
            identifier,
            f"Invalid non-char sub-field {field_name} found in {doc_name}",
        )
        if params:
            length = len(value) if value is not None else 0
            _assert(
                length <= params[0],
                identifier,
                f"Invalid sub-field {field_name} length {length} found in {doc_name}",
            )

    elif expected_type == "did_uid":
        identifier = "DID:Database:ValidationFieldUID"
        _assert(
            isinstance(value, str),
            identifier,
            f"Invalid non-UID sub-field {field_name} found in {doc_name}",
        )
        if value == "":
            return
        uid_part = r"[\dA-F]{16}"
        _assert(
            len(value) == 33
            and re.search(f"{uid_part}_{uid_part}", value, flags=re.IGNORECASE)
            is not None,
            identifier,
            f"Invalid non-UID sub-field {field_name} found in {doc_name}",
        )

    elif expected_type == "structure":
        # MATLAB's check is `isempty(value) | isstruct(value)`, and isstruct is
        # true for a struct ARRAY as well as a scalar struct. A 1xN struct array
        # is what MATLAB uses for a list of like-shaped records, and jsonencode
        # writes it as a JSON array of objects -- which arrives here as a list
        # of dicts.
        #
        # Accepting only a dict rejected every such field. It surfaced on
        # stimulus_presentation.stimuli, whose value is the list of stimuli in
        # a presentation: MATLAB stores those documents happily, Python could
        # not store them at all.
        #
        # A list whose entries are not all dicts is not a struct array in
        # MATLAB either -- it would be a numeric or cell array, and isstruct
        # would be false -- so that stays rejected.
        _assert(
            _is_empty(value)
            or isinstance(value, dict)
            or (
                isinstance(value, (list, tuple))
                and all(isinstance(item, dict) for item in value)
            ),
            "DID:Database:ValidationFieldStructure",
            f"Invalid structure sub-field {field_name} found in {doc_name}",
        )

    elif expected_type == "cell":
        _assert(
            _is_empty(value) or isinstance(value, (list, tuple)),
            "DID:Database:ValidationFieldStructure",
            f"Invalid cell sub-field {field_name} found in {doc_name}",
        )

    else:
        _raise(
            "DID:Database:ValidationFieldType",
            f'Invalid sub-field {field_name} type "{definition.get("type", "")}" '
            f"defined in {doc_name}",
        )


def _is_nan_param(param):
    return _is_nan(param) or (isinstance(param, str) and param.lower() == "nan")


def _matrix_shape(value):
    """Shape of a numeric scalar / list / nested list / ndarray, else None."""
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return tuple(value.shape) if value.ndim > 1 else (value.size, 1)
    except ImportError:
        pass

    if _is_numeric(value):
        return (1, 1)
    if isinstance(value, (list, tuple)):
        if not value:
            return (0, 0)
        if all(isinstance(row, (list, tuple)) for row in value):
            widths = {len(row) for row in value}
            if len(widths) != 1:
                return None
            for row in value:
                if not all(_is_numeric(item) for item in row):
                    return None
            return (len(value), widths.pop())
        if all(_is_numeric(item) for item in value):
            return (len(value), 1)
    return None


def _parses_as_timestamp(value):
    text = re.sub(r"Z$", "", value)
    try:
        datetime.fromisoformat(text)
        return True
    except ValueError:
        pass
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            datetime.strptime(text, fmt)  # noqa: DTZ007 - format check only
            return True
        except ValueError:
            continue
    return False


# ---------------------------------------------------------------------------
# File validation
# ---------------------------------------------------------------------------


def is_filename_match(expected_name, actual_name):
    """Do two file names match?

    They match if equal, or if ``expected_name`` ends in '#' and
    ``actual_name`` is that stem followed by digits. Mirrors MATLAB
    ``database.isfilenamematch``.
    """
    if expected_name == actual_name:
        return True
    if expected_name and expected_name[-1] == "#":
        stem = expected_name[:-1]
        if actual_name.startswith(stem):
            rest = actual_name[len(expected_name) - 1 :]
            return len(rest) > 0 and rest.isdigit()
    return False


def can_find_one_file(locations):
    """Can at least one location for a file be found?

    A local path must exist. Any non-local location -- an http(s) URL, an
    ``s3://`` key, anything else -- is not pre-checked and counts as findable;
    its existence is evaluated when the file is actually read.

    Validation does no network I/O. An unreachable URL is therefore reported
    when the file is read, not when the document is added, which is the same
    treatment every other non-local scheme already got.

    Mirrors MATLAB ``database.canfindonefile``. Both sides changed on
    2026-08-28: MATLAB previously tried to HEAD-check an http location, so a
    required URL-hosted file was rejected at validation with "Missing file".
    That branch had never worked (it passed an unassigned ``url`` variable and
    swallowed the error), and singling out http was inconsistent with every
    other non-local scheme, so the pre-check was removed rather than repaired.
    """
    for location in _as_list(locations):
        if isinstance(location, dict):
            path = location.get("location", "")
        else:
            path = location
        if not isinstance(path, str):
            continue
        if os.path.isfile(path):
            return True
        # Not a local file: not pre-checked here, resolved at read time.
        return True
    return False


def check_files(
    expected_names,
    must_have_value,
    actual_file_names,
    doc_name,
    files,
    actual_file_list,
):
    """Check that offered files match those expected or required.

    Returns ``(is_valid, error_message)``. Mirrors MATLAB
    ``database.checkfiles``.
    """
    expected_unique = sorted(set(expected_names))
    actual_unique = sorted(set(actual_file_names))

    missing = [n for n in expected_unique if n not in list(actual_file_list)]
    if missing:
        return False, (
            f"Some required files are missing (including {missing[0]}) from the "
            f"file_list in document {doc_name}"
        )

    for actual in actual_unique:
        if actual in expected_names:
            continue
        if any(is_filename_match(expected, actual) for expected in expected_unique):
            continue
        return (
            False,
            f"Dissimilar files defined/found (including {actual}) for {doc_name}",
        )

    for index, expected_value in enumerate(must_have_value):
        if not _is_truthy(expected_value):
            continue
        item_name = expected_names[index]
        for file_index, actual in enumerate(actual_file_names):
            if not is_filename_match(item_name, actual):
                continue
            entry = files[file_index] if file_index < len(files) else {}
            locations = entry.get("locations") if isinstance(entry, dict) else None
            if not can_find_one_file(locations):
                return False, f"Missing file {item_name} in {doc_name}"

    return True, ""


# ---------------------------------------------------------------------------
# The validator proper
# ---------------------------------------------------------------------------


def validate_doc_vs_schema(doc_props, schema, all_ids, debug=False):
    """Validate one document against one schema.

    ``all_ids`` is the lowercased superset of document ids in the database and
    in the batch being added; dependency values must resolve into it. Raises
    :class:`ValidationError` on the first failure. Mirrors MATLAB
    ``database.validate_doc_vs_schema``.
    """
    doc_id = (doc_props.get("base") or {}).get("id", "")
    class_props = doc_props.get("document_class")
    if not isinstance(class_props, dict):
        _raise(
            "DID:Database:MissingRequiredField",
            f"Doc {doc_id} document_properties has no document_class field!",
        )
    class_name = class_props.get("class_name")
    if class_name is None:
        _raise(
            "DID:Database:MissingRequiredField",
            f"Doc {doc_id} document_class has no class_name field!",
        )

    doc_name = f"{class_name} doc {doc_id}"
    schema_class_name = schema.get("classname", "")
    if IGNORE_DID_CLASS_PREFIX:
        class_name = _strip_did_prefix(class_name)
        schema_class_name = _strip_did_prefix(schema_class_name)

    is_superclass = class_name.lower() != schema_class_name.lower()
    if is_superclass:
        doc_name = f"{doc_name} (superclass {schema_class_name})"
    if debug:
        print(f"Validating {doc_name}")

    super_full_names = _superclass_definitions(class_props)
    super_names = _superclass_names(class_props)

    for field in schema:
        expected = schema[field]

        if field == "classname":
            expected_name = (
                _strip_did_prefix(expected) if IGNORE_DID_CLASS_PREFIX else expected
            )
            candidates = [n.lower() for n in super_names] + [class_name.lower()]
            _assert(
                str(expected_name).lower() in candidates,
                "DID:Database:ValidationClassname",
                f'Mismatched classname ("{expected_name}" <=> "{class_name}") in doc {doc_id}',
            )

        elif field == "superclasses":
            if is_superclass:
                continue
            expected_str = ",".join(sorted({str(e) for e in _as_list(expected)}))
            found_str = ",".join(super_names)
            _assert(
                expected_str.lower() == found_str.lower(),
                "DID:Database:ValidationSuperClasses",
                f"Dissimilar superclasses defined/found for {doc_name} "
                f'("{expected_str}" <=> "{found_str}")',
            )
            # Recursively validate the document against each superclass schema.
            for definition in super_full_names:
                def_struct = get_document_schema(definition)
                validation_file = (def_struct.get("document_class") or {}).get(
                    "validation", ""
                )
                if validation_file:
                    super_schema = get_document_schema(validation_file)
                    validate_doc_vs_schema(
                        doc_props, super_schema, all_ids, debug=debug
                    )

        elif field == "depends_on":
            _validate_depends_on(
                doc_props, expected, all_ids, doc_name, class_name, is_superclass
            )

        elif field == "file":
            _validate_files(doc_props, expected, doc_name, is_superclass)

        else:
            _validate_class_fields(doc_props, field, expected, doc_name)


def _validate_depends_on(
    doc_props, expected, all_ids, doc_name, class_name, is_superclass
):
    if _is_empty(expected) and is_superclass:
        return

    depends = _doc_dependency_entries(doc_props)
    doc_names = [d.get("name", "") for d in depends]
    if _is_empty(expected) and not doc_names:
        return

    # The schema declares un-enumerated stems; documents may enumerate.
    doc_names_alt = [_strip_enumeration_suffix(n) for n in doc_names]
    doc_names_alt_lower = [n.lower() for n in doc_names_alt]

    expected_entries = [e for e in _as_list(expected) if isinstance(e, dict)]
    expected_names = [e.get("name", "") for e in expected_entries]
    must_have_value = [e.get("mustbenotempty") for e in expected_entries]

    # Only dependencies the schema marks mustbenotempty are required to be
    # present. Optional dependencies may be omitted from the document without
    # being an error.
    is_required = [_is_truthy(v) for v in must_have_value]
    required_names = [n for n, req in zip(expected_names, is_required) if req]
    are_same = all(n.lower() in doc_names_alt_lower for n in set(required_names))

    # Report optional dependencies that are declared but absent. Off by default
    # so it does not surface in normal releases; enable it by setting
    # DID_FORCE_VALIDATION_WARNINGS to a non-zero value. When enabled the
    # warning is forced through even if the caller has suppressed warnings
    # globally, and the prior filter state is restored afterwards.
    optional_names = [n for n, req in zip(expected_names, is_required) if not req]
    missing_optional = [
        n for n in optional_names if n.lower() not in doc_names_alt_lower
    ]
    if missing_optional and forced_warnings_enabled():
        with warnings.catch_warnings():
            warnings.simplefilter("always", MissingOptionalDependencyWarning)
            warnings.warn(
                f"Optional dependency(ies) {{{', '.join(missing_optional)}}} missing "
                f'from document of class "{class_name}"',
                MissingOptionalDependencyWarning,
                stacklevel=2,
            )

    if not are_same:
        _raise(
            "DID:Database:ValidationDependsOn",
            f"Dissimilar dependencies defined/found for {doc_name}.\n\n"
            f"Expected dependencies: {{{', '.join(expected_names)}}}\n"
            f"Found dependencies:    {{{', '.join(doc_names_alt)}}}",
        )

    # Every declared dependency that must have a value must have one, and every
    # non-empty dependency value must resolve to a known document id.
    for index, item_name in enumerate(expected_names):
        value = None
        for dependency in depends:
            if str(dependency.get("name", "")).lower() == str(item_name).lower():
                value = dependency.get("value")
                break

        if is_required[index]:
            _assert(
                not _is_empty(value),
                "DID:Database:ValidationDependEmpty",
                f'Empty dependency found for "{item_name}" in {doc_name}',
            )

        if not _is_empty(value):
            _assert(
                isinstance(value, str),
                "DID:Database:ValidationDependNotACharacterArray",
                f'Non-character dependency value entered for "{item_name}" in {doc_name}',
            )
            _assert(
                value.lower() in all_ids,
                "DID:Database:ValidationDependency",
                f'Dependent doc ID "{value}" ({item_name}) of {doc_name} not found '
                f"in the database or input docs",
            )


def _validate_files(doc_props, expected, doc_name, is_superclass):
    files_prop = doc_props.get("files") or {}
    file_info = [
        f for f in _as_list(files_prop.get("file_info")) if isinstance(f, dict)
    ]
    actual_file_names = [str(f.get("name", "")) for f in file_info]
    file_list = [str(n) for n in _as_list(files_prop.get("file_list"))]

    if _is_empty(expected) and (is_superclass or not actual_file_names):
        return

    expected_entries = [e for e in _as_list(expected) if isinstance(e, dict)]
    expected_names = [e.get("name", "") for e in expected_entries]
    must_have_value = [e.get("mustbenotempty") for e in expected_entries]

    is_valid, error_message = check_files(
        expected_names,
        must_have_value,
        actual_file_names,
        doc_name,
        file_info,
        file_list,
    )
    _assert(is_valid, "DID:Database:ValidationFiles", error_message)


def _validate_class_fields(doc_props, field, expected, doc_name):
    if field not in doc_props:
        _raise(
            "DID:Database:PropertyFieldMissing",
            f"Reference to non-existent field {field} in {doc_name}",
        )
    doc_value = doc_props[field]
    if _is_empty(expected):
        return

    expected_entries = [e for e in _as_list(expected) if isinstance(e, dict)]
    expected_sub_fields = ",".join(
        sorted({str(e.get("name", "")) for e in expected_entries})
    )
    doc_sub_fields = ",".join(
        sorted({str(k) for k in doc_value}) if isinstance(doc_value, dict) else []
    )
    _assert(
        expected_sub_fields.lower() == doc_sub_fields.lower(),
        "DID:Database:ValidationFields",
        f"Dissimilar sub-fields defined/found for {field} field in {doc_name} "
        f'(expected fields "{expected_sub_fields}" <=> actual fields "{doc_sub_fields}")',
    )

    for definition in expected_entries:
        sub_field = definition.get("name", "")
        validate_field_type_and_value(
            doc_name, f"{field}.{sub_field}", doc_value.get(sub_field), definition
        )


def validate_docs(document_objs, all_ids, debug=False):
    """Validate a batch of documents against their schemas.

    ``document_objs`` may be Document objects or raw property dicts. A document
    whose ``document_class`` has no ``validation`` field is skipped -- that is
    how MATLAB opts a class out of validation. Mirrors MATLAB
    ``database.validate_docs``.
    """
    for doc in document_objs:
        doc_props = getattr(doc, "document_properties", doc)
        doc_id = (doc_props.get("base") or {}).get("id", "")

        if "document_class" not in doc_props:
            _raise(
                "DID:Database:MissingRequiredField",
                f"Doc {doc_id} document_properties has no document_class field!",
            )
        class_props = doc_props["document_class"]
        _assert(
            not _is_empty(class_props),
            "DID:Database:MissingRequiredField",
            f"Doc {doc_id} document_class field is empty!",
        )
        for required in ("class_name", "property_list_name", "class_version"):
            if required not in class_props:
                _raise(
                    "DID:Database:MissingRequiredField",
                    f"Doc {doc_id} document_class has no {required} field!",
                )
            _assert(
                not _is_empty(class_props[required]),
                "DID:Database:MissingRequiredField",
                f"Doc {doc_id} {required} field is empty!",
            )

        schema_filename = class_props.get("validation")
        if _is_empty(schema_filename):
            continue  # no validation field, so don't validate this doc

        schema = get_document_schema(schema_filename)
        validate_doc_vs_schema(doc_props, schema, all_ids, debug=debug)
