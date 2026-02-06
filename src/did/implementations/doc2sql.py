def get_field(doc_props, fields):
    if not isinstance(fields, list):
        fields = [fields]

    for field in fields:
        path = field.split('.')
        d = doc_props
        try:
            for p in path:
                d = d[p]
            if d:
                return d
        except (KeyError, TypeError):
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

def doc_to_sql(doc):
    doc_props = doc.document_properties

    sql_meta_data = {
        'name': 'meta',
        'columns': []
    }

    id_val = get_field(doc_props, ['base.id', 'ndi_document.id'])
    sql_meta_data['columns'].append(new_column('doc_id', id_val))

    class_name = get_field(doc_props, ['document_class.class_name', 'ndi_document.type'])
    sql_meta_data['columns'].append(new_column('class', class_name))

    # Simplified superclass and dependency handling
    # A full implementation would parse these structures more carefully.
    sql_meta_data['columns'].append(new_column('superclass', ''))
    sql_meta_data['columns'].append(new_column('depends_on', ''))

    datestamp = get_field(doc_props, ['base.datestamp', 'ndi_document.datestamp'])
    sql_meta_data['columns'].append(new_column('datestamp', datestamp))

    # Process other fields
    other_meta_data = []
    for field_name, field_value in doc_props.items():
        if field_name not in ['base', 'document_class', 'depends_on', 'files']:
            meta_table = {
                'name': field_name,
                'columns': [new_column('doc_id', id_val)]
            }
            if isinstance(field_value, dict):
                for sub_field_name, sub_field_value in field_value.items():
                    meta_table['columns'].append(new_column(sub_field_name, sub_field_value))
            other_meta_data.append(meta_table)

    return [sql_meta_data] + other_meta_data