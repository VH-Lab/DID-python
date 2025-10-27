import json
import numpy as np

def cell_to_str(the_cell):
    """
    Converts a 1-D list to a string representation.
    """
    if not the_cell:
        return '[]'

    return str(the_cell)

def cell_or_item(var, index=0, use_index_for_var=False):
    """
    Returns the ith element of a list, or a single item.
    """
    if isinstance(var, list):
        return var[index]
    else:
        if use_index_for_var:
            return var[index]
        else:
            return var

def empty_struct(*field_names):
    """
    Creates an empty dictionary with given field names.
    """
    if not field_names:
        return {}

    if isinstance(field_names[0], list):
        field_names = field_names[0]

    return {name: None for name in field_names}

def json_encode_nan(obj, **kwargs):
    """
    Encodes a Python object into a JSON object, allowing for NaN/Infinity.
    """
    return json.dumps(obj, allow_nan=True, **kwargs)

def is_full_field(a, composite_field_name):
    """
    Checks if a field (or field and subfield) of a dictionary exists.
    """
    field_names = composite_field_name.split('.')

    current_level = a
    for name in field_names:
        if isinstance(current_level, dict) and name in current_level:
            current_level = current_level[name]
        else:
            return False, None

    return True, current_level

def eq_emp(x, y):
    """
    If both X and Y are not empty, returns X==Y. If both X and Y are empty, b=1.
    Otherwise, b=0.
    """
    if not x and not y:
        return True
    if not x or not y:
        return False
    return x == y

def eq_tot(x, y):
    """
    Returns EQEMP(X,Y), except that if the result is an array of boolean values,
    the logical AND of all the results is returned.
    """
    return np.all(eq_emp(x, y))

def size_eq(x, y):
    """
    Determines if size of two variables is same.
    """
    return np.shape(x) == np.shape(y)

def eq_len(x, y):
    """
    Returns 1 if objects to compare are equal and have same size.
    """
    return size_eq(x, y) and eq_tot(x, y)

def field_search(a, search_struct):
    """
    Searches a dictionary to determine if it matches a search structure.
    """
    if isinstance(search_struct, list):
        return all(field_search(a, s) for s in search_struct)

    b = False

    negation = search_struct['operation'].startswith('~')
    op = search_struct['operation'].lstrip('~')

    if op == 'isa':
        # In the Matlab version, this checks all superclasses.
        # For now, we will just check the direct class name.
        is_there, value = is_full_field(a, 'document_class.class_name')
        if is_there:
            b = value == search_struct['param1']
    elif op == 'or':
        b = field_search(a, search_struct['param1']) or field_search(a, search_struct['param2'])
    else:
        is_there, value = is_full_field(a, search_struct['field'])
        if op == 'regexp':
            if is_there and isinstance(value, str):
                import re
                b = bool(re.search(search_struct['param1'], value))
        elif op == 'exact_string':
            if is_there:
                b = value == search_struct['param1']

    return not b if negation else b

def get_demo_type(doc):
    """
    Finds the first demo class for a given document.
    """
    if 'demoA' in doc.document_properties:
        return 'demoA'
    elif 'demoB' in doc.document_properties:
        return 'demoB'
    elif 'demoC' in doc.document_properties:
        return 'demoC'
    return ''
