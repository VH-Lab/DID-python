"""Document construction against a corpus that ships schemas but no definitions.

``read_blank_definition`` documents a fallback "for callers that only ship a
schema". That path was broken: ``base.schema.json`` stores ``base`` as a LIST
of field descriptors, and ``Document.__init__`` immediately subscripts it as a
dict to stamp ``base.id``, so constructing anything raised
``TypeError: list indices must be integers or slices, not str``.

Regression for the half of DID-python#30 that DID-python#37 did not carry.
#30 guarded only the ``database_schema`` branch; a corpus whose definition
directories point straight at the schema files is served by the flat-schema
branch above it, so these cases still crashed. Both branches are guarded now.
"""

import json
import shutil
from pathlib import Path

import pytest

from did.common import PathConstants
from did.document import Document

BUILTIN_SCHEMA = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "did"
    / "example_schema"
    / "demo_schema1"
)


@pytest.fixture
def schema_only_corpus(tmp_path, monkeypatch):
    """A corpus with database_schema/ but no database_documents/.

    The definition directories point at the schema files themselves, so
    ``read_json_file_location`` finds a schema-shaped file rather than
    returning None -- the flat-schema branch, which is the one #30 missed.
    """
    corpus = tmp_path / "schema_only"
    shutil.copytree(BUILTIN_SCHEMA / "database_schema", corpus / "database_schema")

    monkeypatch.setattr(PathConstants, "DEFPATH", str(corpus))
    monkeypatch.setattr(
        PathConstants, "DEFINITIONS", {"$DIDSCHEMA": [str(corpus / "database_schema")]}
    )
    return corpus


def test_base_schema_really_stores_base_as_a_list(schema_only_corpus):
    """Guard the premise: if this shape changes, the tests below prove nothing."""
    raw = json.loads(
        (schema_only_corpus / "database_schema" / "base.schema.json").read_text()
    )
    assert isinstance(raw["base"], list)
    assert all("name" in field for field in raw["base"])


def test_document_base_is_constructible(schema_only_corpus):
    doc = Document("base")
    base = doc.document_properties["base"]
    assert isinstance(base, dict)
    # The descriptor names became keys, and __init__ stamped the two it owns.
    assert "session_id" in base
    assert base["id"]
    assert base["datestamp"]


def test_default_document_is_constructible(schema_only_corpus):
    """Document() defaults to document_type='base', so it took the same path."""
    assert isinstance(Document().document_properties["base"], dict)


def test_descriptor_defaults_are_carried_through(schema_only_corpus):
    raw = json.loads(
        (schema_only_corpus / "database_schema" / "base.schema.json").read_text()
    )
    expected = {
        f["name"]: f.get("default_value", "")
        for f in raw["base"]
        if f["name"] not in ("id", "datestamp")  # stamped by __init__
    }
    base = Document("base").document_properties["base"]
    for name, default in expected.items():
        assert base[name] == default


class TestBlankBaseGroup:
    """The normalizer itself, over every shape it can be handed."""

    def test_list_becomes_a_defaults_dict(self):
        descriptors = [
            {"name": "id", "default_value": ""},
            {"name": "name", "default_value": "unnamed"},
        ]
        assert Document._blank_base_group(descriptors) == {"id": "", "name": "unnamed"}

    def test_descriptor_without_default_value_becomes_empty_string(self):
        assert Document._blank_base_group([{"name": "id"}]) == {"id": ""}

    def test_malformed_descriptor_is_dropped(self):
        assert Document._blank_base_group([{"no_name": 1}, "junk"]) == {}

    def test_dict_passes_through_unchanged(self):
        base = {"id": "abc"}
        assert Document._blank_base_group(base) is base

    def test_missing_or_unusable_becomes_empty_dict(self):
        assert Document._blank_base_group(None) == {}
        assert Document._blank_base_group("nonsense") == {}
