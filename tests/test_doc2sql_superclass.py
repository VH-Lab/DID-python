"""Tests for _get_superclass_str handling of bare dict superclasses.

Regression tests for https://github.com/Waltham-Data-Science/NDI-python/issues/52
"""

from did.implementations.doc2sql import _get_superclass_str


class TestGetSuperclassStrBareDict:
    """Bare dict superclasses (from MATLAB's jsonencode) should be handled."""

    def test_top_level_bare_dict(self):
        doc_props = {"superclasses": {"definition": "$NDIDOCUMENTPATH/base.json"}}
        assert _get_superclass_str(doc_props) == "base"

    def test_top_level_list_single(self):
        doc_props = {"superclasses": [{"definition": "$NDIDOCUMENTPATH/base.json"}]}
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


class TestGetSuperclassStrDid2ClassName:
    """did2 (V_delta / V_zeta) superclasses carry ``class_name``, not ``definition``.

    Ports DID-matlab c618a05. MATLAB errored outright ("Unrecognized field name
    definition") on such a document; Python silently dropped the entry, writing
    an empty ``meta.superclass`` column, so a later ``isa()`` query returned a
    different result set in each language with no error in either.
    """

    def test_document_class_class_name_list(self):
        doc_props = {
            "document_class": {
                "superclasses": [{"class_name": "base"}, {"class_name": "demoA"}]
            }
        }
        assert _get_superclass_str(doc_props) == "base, demoA"

    def test_document_class_class_name_bare_dict(self):
        doc_props = {"document_class": {"superclasses": {"class_name": "base"}}}
        assert _get_superclass_str(doc_props) == "base"

    def test_top_level_class_name(self):
        doc_props = {"superclasses": [{"class_name": "base"}]}
        assert _get_superclass_str(doc_props) == "base"

    def test_definition_wins_over_class_name(self):
        """MATLAB reads ``{superclass.definition}`` across the whole struct array.

        The choice of key is made once for the collection, so an entry carrying
        only ``class_name`` alongside entries carrying ``definition`` is dropped
        in MATLAB too. Matching that is deliberate: a struct array cannot hold
        mixed fields, so this shape does not arise from MATLAB, and inventing a
        per-entry union here would diverge from the language being mirrored.
        """
        doc_props = {
            "document_class": {
                "superclasses": [
                    {"definition": "$DIDDOCUMENT/base.json"},
                    {"class_name": "demoA"},
                ]
            }
        }
        assert _get_superclass_str(doc_props) == "base"

    def test_unrecognized_entry_shape_is_dropped(self):
        doc_props = {"document_class": {"superclasses": [{"unrelated": "x"}]}}
        assert _get_superclass_str(doc_props) == ""
