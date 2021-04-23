"""Utilities"""
from __future__ import annotations
import did.types as T
import re

# Captures "words" in pascal case
pascal_pattern = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')


def pascal_to_snake_case(string: str) -> str:
    """Converts  PascalCase strings to snake_case

    :param string: String without whitespace.
    :type string: str
    :return: String in snake case.
    :rtype: str
    """
    return pascal_pattern.sub(r'_\1', string).lower()


def class_to_collection_name(ndi_class) -> str:
    """Convert a :class:`ndi.ndi_class` __name__ (PascalCase, singular) to a collection name (snake_case, plural).
    .. note::
        For consistency, collection names are made plural by the addition of an 's'.

    :param string: An :class:`ndi.ndi_class` object.
    :type string: :class:`ndi.ndi_class`
    :return: A standardized collection name for the given class.
    :rtype: str
    """
    return f'{pascal_to_snake_case(ndi_class.__name__)}s'


def flatten(nested_list: T.List[T.List]) -> T.List:
    """[summary]

    :param nested_list: [description]
    :type nested_list: [type]
    """
    return [item for l in nested_list for item in l]


def generate_tuple_analog(schema_enum) -> tuple:
    """Produces a tuple from a flatbuffer schema enum class. The generated tuple is used in an :term:`DID class`\ 's _reconstruct method.
    ::
        class SchemaType(object):
            abc = 0
            def = 1
            ghi = 2

        DID_Type = generate_tuple_analog(SchemaType)
        # DID_Type = ('abc', 'def', 'ghi')

        t = SchemaType.def
        DID_enum[t]
        # returns 'def'

    :param schema_enum: [description]
    :type schema_enum: [type]
    :return: [description]
    :rtype: [type]
    """

    enum_map = {}
    max_index = 0
    for key, value in schema_enum.__dict__.items():
        if isinstance(value, int):
            enum_map[value] = key
            if value > max_index:
                max_index = value

    enum_list = []
    for i in range(max_index + 1):
        enum_list.append(enum_map[i])

    return tuple(enum_list)

def has_single_snapshot(document):
    return len(document.data['base']['snapshots']) == 0