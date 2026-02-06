import json
import numpy as np
import re

def cell_to_str(the_list):
    """
    Converts a 1-D list to a string representation.

    This function mimics the behavior of the Matlab `cell2str` function.
    """
    if not the_list:
        return '[]'

    return json.dumps(the_list)

def cell_or_item(var, index=0, use_index_for_var=False):
    """
    Returns the ith element of a list, or a single item.

    This function mimics the behavior of the Matlab `celloritem` function.
    """
    if isinstance(var, list):
        return var[index]
    else:
        if use_index_for_var:
            return var[index]
        else:
            return var

def col_vec(x):
    """
    Returns a matrix reshaped as a column vector.

    This function mimics the behavior of the Matlab `colvec` function.
    """
    return np.array(x).flatten('F').tolist()

def empty_struct(*field_names):
    """
    Creates a structure with given fieldnames that is empty.

    This function mimics the behavior of the Matlab `emptystruct` function.
    In Python, this returns an empty dictionary.
    """
    return {}

def is_empty(x):
    """
    Checks if a value is empty (None or an empty container).
    """
    if x is None:
        return True
    try:
        return len(x) == 0
    except TypeError:
        return False

def eq_emp(x, y):
    """
    Compares two values, with special handling for empty values.

    This function mimics the behavior of the Matlab `eqemp` function.
    """
    x_empty = is_empty(x)
    y_empty = is_empty(y)

    if x_empty and y_empty:
        return True
    elif x_empty or y_empty:
        return False
    else:
        return x == y

def size_eq(x, y):
    """
    Determines if the size of two variables is the same.

    This function mimics the behavior of the Matlab `sizeeq` function.
    """
    return np.array(x).shape == np.array(y).shape

def eq_tot(x, y):
    """
    Returns the logical AND of all the results of an element-wise comparison.

    This function mimics the behavior of the Matlab `eqtot` function.
    """
    return np.array_equal(x, y)

def eq_len(x, y):
    """
    Returns True if objects to compare are equal and have the same size.

    This function mimics the behavior of the Matlab `eqlen` function.
    """
    if size_eq(x, y):
        return eq_tot(x, y)
    else:
        return False

def eq_unique(in_list):
    """
    Return unique elements of a list.

    This function mimics the behavior of the Matlab `equnique` function.
    """
    out_list = []
    for item in in_list:
        is_unique = True
        for out_item in out_list:
            if eq_len(item, out_item):
                is_unique = False
                break
        if is_unique:
            out_list.append(item)
    return out_list

def is_full_field(a, composite_field_name):
    """
    Checks if a nested field exists in a dictionary.

    This function mimics the behavior of the Matlab `isfullfield` function.
    """
    if not isinstance(a, dict):
        return False, None

    field_names = composite_field_name.split('.')
    current_level = a

    for field_name in field_names:
        if isinstance(current_level, dict) and field_name in current_level:
            current_level = current_level[field_name]
        else:
            return False, None

    return True, current_level

def struct_partial_match(a, b):
    """
    Checks if dictionary b is a subset of dictionary a.
    """
    if not isinstance(a, dict) or not isinstance(b, dict):
        return False

    for key, value in b.items():
        if key not in a or a[key] != value:
            return False

    return True

def field_search(a, search_struct):
    """
    Searches a dictionary to determine if it matches a search structure.

    This function mimics the behavior of the Matlab `fieldsearch` function.
    """
    if isinstance(search_struct, list):
        # AND operation
        return all(field_search(a, s) for s in search_struct)

    b = False  # Assume no match initially

    field = search_struct.get('field', '')
    operation = search_struct.get('operation', '')
    param1 = search_struct.get('param1')
    param2 = search_struct.get('param2')

    is_there, value = is_full_field(a, field) if field else (True, a)

    negation = False
    if operation.startswith('~'):
        negation = True
        operation = operation[1:]

    op_lower = operation.lower()

    if op_lower == 'regexp':
        if is_there and isinstance(value, str):
            if re.search(param1, value):
                b = True
    elif op_lower == 'exact_string':
        if is_there:
            b = (value == param1)
    elif op_lower == 'exact_string_anycase':
        if is_there and isinstance(value, str):
            b = (value.lower() == param1.lower())
    elif op_lower == 'contains_string':
        if is_there and isinstance(value, str):
            b = (param1 in value)
    elif op_lower == 'exact_number':
        if is_there:
            b = eq_len(value, param1)
    elif op_lower == 'lessthan':
        if is_there:
            try:
                b = np.all(np.array(value) < param1)
            except (ValueError, TypeError):
                pass
    elif op_lower == 'lessthaneq':
        if is_there:
            try:
                b = np.all(np.array(value) <= param1)
            except (ValueError, TypeError):
                pass
    elif op_lower == 'greaterthan':
        if is_there:
            try:
                b = np.all(np.array(value) > param1)
            except (ValueError, TypeError):
                pass
    elif op_lower == 'greaterthaneq':
        if is_there:
            try:
                b = np.all(np.array(value) >= param1)
            except (ValueError, TypeError):
                pass
    elif op_lower == 'hassize':
        if is_there:
            b = eq_len(np.array(value).shape, param1)
    elif op_lower == 'hasmember':
        if is_there:
            try:
                b = param1 in value
            except TypeError:
                pass
    elif op_lower == 'hasfield':
        b = is_there
    elif op_lower == 'partial_struct':
        if is_there:
            b = struct_partial_match(value, param1)
    elif op_lower in ('hasanysubfield_contains_string', 'hasanysubfield_exact_string'):
        if is_there and (isinstance(value, list) or isinstance(value, dict)):
            items_to_check = value if isinstance(value, list) else [value]
            param1_list = param1 if isinstance(param1, list) else [param1]
            param2_list = param2 if isinstance(param2, list) else [param2]

            for item in items_to_check:
                if isinstance(item, dict):
                    match = True
                    for p1, p2 in zip(param1_list, param2_list):
                        sub_is_there, sub_value = is_full_field(item, p1)
                        if not sub_is_there:
                            match = False
                            break
                        if op_lower == 'hasanysubfield_contains_string':
                            if not (isinstance(sub_value, str) and p2 in sub_value):
                                match = False
                                break
                        elif op_lower == 'hasanysubfield_exact_string':
                            if not (isinstance(sub_value, str) and sub_value == p2):
                                match = False
                                break
                    if match:
                        b = True
                        break
    elif op_lower == 'or':
        if isinstance(param1, dict) and isinstance(param2, dict):
            b = field_search(a, param1) or field_search(a, param2)
    else:
        raise ValueError(f"Unknown search operation: {operation}")

    return not b if negation else b

def find_closest(arr, v):
    """
    Finds the closest value in an array (using absolute value).

    This function mimics the behavior of the Matlab `findclosest` function.
    """
    if not arr:
        return None, None
    arr = np.asarray(arr)
    idx = (np.abs(arr - v)).argmin()
    return idx, arr[idx]

def json_encode_nan(obj):
    """
    Encodes a Python object into a JSON object, allowing for NaN/Infinity.

    This function mimics the behavior of the Matlab `jsonencodenan` function.
    """
    return json.dumps(obj, allow_nan=True, indent=4)

def struct_merge(s1, s2, error_if_new_field=False, do_alphabetical=True):
    """
    Merges two dictionaries into a common dictionary.

    This function mimics the behavior of the Matlab `structmerge` function.
    """
    if error_if_new_field:
        missing_fields = set(s2.keys()) - set(s1.keys())
        if missing_fields:
            raise ValueError(f"Some fields of the second dictionary are not in the first: {', '.join(missing_fields)}")

    s_out = s1.copy()
    s_out.update(s2)

    if do_alphabetical:
        return {key: s_out[key] for key in sorted(s_out)}
    else:
        return s_out