"""Tests for _flatten_dict list-of-dict (struct-array) expansion.

DID-matlab getMetaTableFrom expands a struct array into one column per element,
suffixed '_<1-based idx>' only when numElements > 1. The Python port previously
str()-repr'd any list into a single column, so per-element fields were
unsearchable and the stored string disagreed with DID-matlab.
"""

from did.implementations.doc2sql import _flatten_dict, doc_to_sql
from did.document import Document


def _cols(items):
    return dict(items)


class TestFlattenStructArray:
    def test_multi_element_list_is_suffixed(self):
        items = _flatten_dict({"channel": [{"name": "ch1"}, {"name": "ch2"}]})
        cols = _cols(items)
        assert cols["channel___name_1"] == "ch1"
        assert cols["channel___name_2"] == "ch2"
        # No unsuffixed collision column.
        assert "channel___name" not in cols

    def test_single_element_list_is_unsuffixed(self):
        # numElements == 1 -> no '_1' suffix (the rule easiest to get wrong).
        items = _flatten_dict({"channel": [{"name": "ch1"}]})
        cols = _cols(items)
        assert cols == {"channel___name": "ch1"}

    def test_nested_dict_within_element(self):
        items = _flatten_dict(
            {"channel": [{"loc": {"x": 1}}, {"loc": {"x": 2}}]}
        )
        cols = _cols(items)
        assert cols["channel___loc___x_1"] == 1
        assert cols["channel___loc___x_2"] == 2

    def test_scalar_list_is_json_not_repr(self):
        # A residual scalar list must be JSON (double quotes), not a Python repr.
        items = _flatten_dict({"tags": ["a", "b"]})
        cols = _cols(items)
        assert cols["tags"] == '["a", "b"]'

    def test_doc_to_sql_expands_group_struct_array(self):
        doc = Document(
            {
                "base": {"id": "a" * 32, "datestamp": "2020-01-01"},
                "document_class": {"class_name": "thing"},
                "element": {"channel": [{"name": "ch1"}, {"name": "ch2"}]},
            }
        )
        tables = doc_to_sql(doc)
        element = next(t for t in tables if t["name"] == "element")
        names = {c["name"]: c["value"] for c in element["columns"]}
        assert names["channel___name_1"] == "ch1"
        assert names["channel___name_2"] == "ch2"
