import re


def get_field(doc_props, fields):
    if not isinstance(fields, list):
        fields = [fields]

    for field in fields:
        path = field.split('.')
        d = doc_props
        try:
            for p in path:
                d = d[p]
            if d is not None and d != '':
                return d
        except (KeyError, TypeError, IndexError):
            continue
    return ''


def new_column(name, value, matlab_type=None):
    if matlab_type is None:
        matlab_type = type(value).__name__
    return {
        'name': name,
        'matlabType': matlab_type,
        'sqlType': sql_type_of(matlab_type),
        'value': value
    }


def sql_type_of(matlab_type):
    type_map = {
        'bool': 'BOOLEAN',
        'str': 'TEXT',
        'int': 'INTEGER',
        'float': 'REAL'
    }
    return type_map.get(matlab_type, 'BLOB')


def _get_class_name(doc_props):
    """Extract class name from document properties, supporting both DID-python and NDI formats."""
    # DID-python schema format
    if 'classname' in doc_props:
        return doc_props['classname']
    # NDI / MATLAB format
    return get_field(doc_props, ['document_class.class_name', 'ndi_document.type'])


def _get_superclass_str(doc_props):
    """Extract superclass string matching MATLAB's doc2sql format.

    MATLAB produces comma-space separated, sorted unique superclass names.
    For MATLAB-style definitions like "$PATH/base.json", strip path and extension.
    For DID-python style ["base", "demoA"], use directly.
    """
    # DID-python schema format: top-level 'superclasses' list of strings
    if 'superclasses' in doc_props and isinstance(doc_props['superclasses'], list):
        superclasses = doc_props['superclasses']
        if not superclasses:
            return ''
        names = []
        for sc in superclasses:
            if isinstance(sc, str):
                names.append(sc)
            elif isinstance(sc, dict) and 'definition' in sc:
                # MATLAB-style: extract name from definition path
                defn = sc['definition']
                name = re.sub(r'.+/', '', defn)
                name = re.sub(r'\..+$', '', name)
                names.append(name)
        names = sorted(set(names))
        return ', '.join(names)

    # NDI / MATLAB format: document_class.superclasses
    superclasses = get_field(doc_props, ['document_class.superclasses'])
    if isinstance(superclasses, list):
        names = []
        for sc in superclasses:
            if isinstance(sc, dict) and 'definition' in sc:
                defn = sc['definition']
                name = re.sub(r'.+/', '', defn)
                name = re.sub(r'\..+$', '', name)
                names.append(name)
            elif isinstance(sc, str):
                names.append(sc)
        names = sorted(set(names))
        return ', '.join(names)

    return ''


def _serialize_depends_on(doc_props):
    """Serialize depends_on matching MATLAB's format: 'name,value;name,value;'"""
    depends_on = doc_props.get('depends_on', [])
    if not depends_on or not isinstance(depends_on, list):
        return ''

    parts = []
    for dep in depends_on:
        if isinstance(dep, dict):
            name = str(dep.get('name', ''))
            value = str(dep.get('value', ''))
            if name and value:
                parts.append(f'{name},{value};')

    return ''.join(parts)


def _flatten_dict(d, prefix=''):
    """Flatten a nested dict using ___ separator for nested keys (matching MATLAB's getMetaTableFrom)."""
    items = []
    for key, value in d.items():
        col_name = f'{prefix}___{key}' if prefix else key
        if isinstance(value, dict):
            items.extend(_flatten_dict(value, col_name))
        elif isinstance(value, list):
            # Convert lists to string representation
            items.append((col_name, str(value)))
        else:
            items.append((col_name, value))
    return items


def _get_meta_table_from(group_name, doc_id, field_value):
    """Create a meta-table from a field group, matching MATLAB's getMetaTableFrom."""
    table = {
        'name': group_name,
        'columns': [new_column('doc_id', doc_id)]
    }

    if isinstance(field_value, dict):
        for col_name, col_value in _flatten_dict(field_value):
            table['columns'].append(new_column(col_name, col_value))

    return table


# Fields to skip when building per-group meta-tables
_SKIP_FIELDS = {'classname', 'document_class', 'superclasses', 'depends_on', 'files', 'file'}


def doc_to_sql(doc):
    """Convert a document to SQL meta-tables matching MATLAB's did.implementations.doc2sql.

    Returns a list of meta-table dicts. The first is always 'meta' with standard
    columns (doc_id, class, superclass, datestamp, creation, deletion, depends_on).
    Subsequent tables correspond to top-level field groups (e.g., 'base', 'element').

    Each meta-table has:
        'name': the group name
        'columns': list of column dicts with 'name' and 'value'
    """
    doc_props = doc.document_properties

    # Build the 'meta' table
    meta = {'name': 'meta', 'columns': []}

    id_val = get_field(doc_props, ['base.id', 'ndi_document.id'])
    meta['columns'].append(new_column('doc_id', id_val))

    class_name = _get_class_name(doc_props)
    meta['columns'].append(new_column('class', class_name))

    superclass = _get_superclass_str(doc_props)
    meta['columns'].append(new_column('superclass', superclass))

    datestamp = get_field(doc_props, ['base.datestamp', 'ndi_document.datestamp'])
    meta['columns'].append(new_column('datestamp', datestamp))

    meta['columns'].append(new_column('creation', ''))
    meta['columns'].append(new_column('deletion', ''))

    depends_on_str = _serialize_depends_on(doc_props)
    meta['columns'].append(new_column('depends_on', depends_on_str))

    meta_tables = [meta]

    # Build per-group tables for all other top-level dict fields
    for field_name, field_value in doc_props.items():
        if field_name in _SKIP_FIELDS:
            continue
        if isinstance(field_value, dict):
            table = _get_meta_table_from(field_name, id_val, field_value)
            meta_tables.append(table)

    return meta_tables
