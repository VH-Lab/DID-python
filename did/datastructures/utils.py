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
