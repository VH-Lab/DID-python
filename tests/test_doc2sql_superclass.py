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


class TestGetSuperclassStrClassName:
    """Schema-v2 (V_delta / V_epsilon) names superclasses with bare
    ``{"class_name": ...}`` objects. ``_get_superclass_str`` must read
    ``class_name`` first but UNION in any ``definition``-derived name (never
    short-circuit), so ``meta.superclass`` — and both isa paths that read it —
    stay consistent with ndi-cloud-node ``class_lineage.ts`` and NDI-python
    ``ndi.document.doc_superclass``. On today's corpus every entry is
    ``{definition}`` (class_name count = 0), so this branch is purely additive.
    """

    # --- top-level 'superclasses' branch (DID-python schema shape) ---
    def test_top_level_class_name_only(self):
        assert _get_superclass_str({"superclasses": [{"class_name": "base"}]}) == "base"

    def test_top_level_bare_dict_class_name(self):
        assert _get_superclass_str({"superclasses": {"class_name": "base"}}) == "base"

    def test_top_level_class_name_list(self):
        doc_props = {"superclasses": [{"class_name": "base"}, {"class_name": "demoA"}]}
        assert _get_superclass_str(doc_props) == "base, demoA"

    def test_top_level_union_no_shortcircuit(self):
        # CONFORMANCE PIN: class_name AND a *differing* definition -> BOTH names.
        # A short-circuit accessor would drop "base" and return "custom_marker".
        sc = {"class_name": "custom_marker", "definition": "$NDIDOCUMENTPATH/base.json"}
        assert _get_superclass_str({"superclasses": [sc]}) == "base, custom_marker"

    def test_top_level_mixed_agreeing_dedup(self):
        sc = {"class_name": "base", "definition": "$NDIDOCUMENTPATH/base.json"}
        assert _get_superclass_str({"superclasses": [sc]}) == "base"

    def test_top_level_empty_class_name_falls_back(self):
        sc = {"class_name": "", "definition": "$NDIDOCUMENTPATH/base.json"}
        assert _get_superclass_str({"superclasses": [sc]}) == "base"

    # --- document_class.superclasses branch (NDI / MATLAB shape) ---
    def test_document_class_class_name_only(self):
        doc = {"document_class": {"superclasses": [{"class_name": "base"}]}}
        assert _get_superclass_str(doc) == "base"

    def test_document_class_bare_dict_class_name(self):
        doc = {"document_class": {"superclasses": {"class_name": "base"}}}
        assert _get_superclass_str(doc) == "base"

    def test_document_class_union_no_shortcircuit(self):
        doc = {
            "document_class": {
                "superclasses": [
                    {
                        "class_name": "custom_marker",
                        "definition": "$NDIDOCUMENTPATH/base.json",
                    }
                ]
            }
        }
        assert _get_superclass_str(doc) == "base, custom_marker"

    def test_document_class_mixed_shapes(self):
        # A document may mix a v2 entry and a legacy entry in one list.
        doc = {
            "document_class": {
                "superclasses": [
                    {"class_name": "element"},
                    {"definition": "$NDIDOCUMENTPATH/base.json"},
                ]
            }
        }
        assert _get_superclass_str(doc) == "base, element"

    def test_document_class_v1_definition_unchanged(self):
        # Regression guard: the entire current corpus is definition-only.
        doc = {
            "document_class": {
                "superclasses": [
                    {"definition": "$NDIDOCUMENTPATH/element.json"},
                    {"definition": "$NDIDOCUMENTPATH/base.json"},
                ]
            }
        }
        assert _get_superclass_str(doc) == "base, element"
