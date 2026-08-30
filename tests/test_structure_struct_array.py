"""A ``structure`` field may hold a struct array, not just a scalar struct.

MATLAB's check is ``isempty(value) | isstruct(value)``, and ``isstruct`` is
true for a struct ARRAY as well as a scalar struct. A 1xN struct array is how
MATLAB holds a list of like-shaped records, and ``jsonencode`` writes it as a
JSON array of objects, which arrives in Python as a list of dicts.

Accepting only a dict rejected every such field. It surfaced on
``stimulus_presentation.stimuli`` -- the list of stimuli in a presentation --
where MATLAB stores the documents happily and Python could not store them at
all: one rejected document took down a 743-document batch, and 533 further
documents then failed on the per-document retry.
"""

from __future__ import annotations

import pytest

from did.validate import ValidationError, validate_field_type_and_value

STRUCTURE = {
    "name": "stimuli",
    "type": "structure",
    "default_value": [],
    "parameters": "",
}


def _check(value):
    validate_field_type_and_value(
        "doc", "stimulus_presentation.stimuli", value, STRUCTURE
    )


class TestAccepted:
    def test_scalar_struct(self):
        _check({"parameters": {}})

    def test_struct_array(self):
        """The case that was rejected: MATLAB's 1xN struct array."""
        _check([{"parameters": {"angle": 0}}, {"parameters": {"angle": 90}}])

    def test_single_element_struct_array(self):
        _check([{"parameters": {}}])

    def test_empty_is_still_empty(self):
        for value in ([], {}, "", None):
            _check(value)


class TestRejected:
    def test_a_list_that_is_not_a_struct_array(self):
        """In MATLAB this is a numeric array; isstruct is false."""
        with pytest.raises(ValidationError):
            _check([1, 2, 3])

    def test_a_list_of_mixed_entries(self):
        with pytest.raises(ValidationError):
            _check([{"parameters": {}}, 7])

    def test_a_bare_scalar(self):
        with pytest.raises(ValidationError):
            _check(42)

    def test_a_string(self):
        with pytest.raises(ValidationError):
            _check("not a struct")

    def test_the_identifier_is_unchanged(self):
        with pytest.raises(ValidationError) as caught:
            _check(42)
        assert caught.value.identifier == "DID:Database:ValidationFieldStructure"
