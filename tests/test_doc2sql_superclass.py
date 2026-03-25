"""Tests for _get_superclass_str handling of bare dict superclasses.

Regression tests for https://github.com/Waltham-Data-Science/NDI-python/issues/52
"""

from did.implementations.doc2sql import _get_superclass_str


class TestGetSuperclassStrBareDict:
    """Bare dict superclasses (from MATLAB's jsonencode) should be handled."""

    def test_top_level_bare_dict(self):
        doc_props = {
            "superclasses": {"definition": "$NDIDOCUMENTPATH/base.json"}
        }
        assert _get_superclass_str(doc_props) == "base"

    def test_top_level_list_single(self):
        doc_props = {
            "superclasses": [{"definition": "$NDIDOCUMENTPATH/base.json"}]
        }
        assert _get_superclass_str(doc_props) == "base"

    def test_document_class_bare_dict(self):
        doc_props = {
            "document_class": {
                "superclasses": {"definition": "$NDIDOCUMENTPATH/base.json"}
            }
        }
        assert _get_superclass_str(doc_props) == "base"

    def test_document_class_list(self):
        doc_props = {
            "document_class": {
                "superclasses": [
                    {"definition": "$NDIDOCUMENTPATH/base.json"},
                    {"definition": "$NDIDOCUMENTPATH/demoA.json"},
                ]
            }
        }
        assert _get_superclass_str(doc_props) == "base, demoA"

    def test_empty_superclasses(self):
        doc_props = {"superclasses": []}
        assert _get_superclass_str(doc_props) == ""

    def test_no_superclasses(self):
        doc_props = {}
        assert _get_superclass_str(doc_props) == ""
