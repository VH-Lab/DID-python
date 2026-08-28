"""Document-vs-schema validation.

Mirrors the MATLAB coverage in tests/+did/+unittest/TestOptionalDependencyWarning.m
and the depends_on rows of TestValidModification / TestInvalidModification, plus
the field type/value checks from did.database.validate_field_type_and_value.
"""

import os
import tempfile
import unittest
import warnings

from did.document import Document
from did.ido import IDO
from did.implementations.sqlitedb import SQLiteDB
from did.validate import (
    MissingOptionalDependencyWarning,
    ValidationError,
    is_filename_match,
    validate_field_type_and_value,
)

ENV_VAR = "DID_FORCE_VALIDATION_WARNINGS"


class _DatabaseTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "test_validation.sqlite"))
        self.db.add_branch("a")
        self._original_env = os.environ.pop(ENV_VAR, None)

    def tearDown(self):
        self.db._close_db()
        if self._original_env is None:
            os.environ.pop(ENV_VAR, None)
        else:
            os.environ[ENV_VAR] = self._original_env


class TestValidDocuments(_DatabaseTestCase):
    def test_valid_doc_is_added(self):
        doc = Document("demoA", **{"demoA.value": 5})
        self.db.add_docs([doc])
        self.assertIsNotNone(self.db.get_docs(doc.id()))

    def test_superclass_property_lists_are_validated(self):
        # demoB's schema declares superclasses [base, demoA], so validation
        # recurses into demoA's schema and the doc must carry a demoA list.
        doc = Document("demoB", **{"demoB.value": 2, "demoA.value": 2})
        self.db.add_docs([doc])
        self.assertIsNotNone(self.db.get_docs(doc.id()))

    def test_dependency_on_doc_in_same_batch(self):
        target = Document("demoA", **{"demoA.value": 1})
        dependent = Document("demoC", **{"demoC.value": 1})
        dependent.set_dependency_value("item1", target.id(), error_if_not_found=False)
        # all_ids is the superset of the database and this batch, so a batch
        # may depend on itself.
        self.db.add_docs([target, dependent])
        self.assertIsNotNone(self.db.get_docs(dependent.id()))

    def test_validate_false_bypasses_validation(self):
        doc = Document("demoA", **{"demoA.value": 999999})
        self.db.add_docs([doc], validate=False)
        self.assertIsNotNone(self.db.get_docs(doc.id()))


class TestInvalidDocuments(_DatabaseTestCase):
    def _assert_rejects(self, doc, identifier):
        with self.assertRaises(ValidationError) as caught:
            self.db.add_docs([doc])
        self.assertEqual(caught.exception.identifier, identifier)

    def test_integer_out_of_range(self):
        self._assert_rejects(
            Document("demoA", **{"demoA.value": 999999}),
            "DID:Database:ValidationFieldInteger",
        )

    def test_integer_non_numeric(self):
        self._assert_rejects(
            Document("demoA", **{"demoA.value": "not a number"}),
            "DID:Database:ValidationFieldInteger",
        )

    def test_integer_non_integral(self):
        self._assert_rejects(
            Document("demoA", **{"demoA.value": 1.5}),
            "DID:Database:ValidationFieldInteger",
        )

    def test_unknown_dependency_id(self):
        doc = Document("demoC", **{"demoC.value": 1})
        doc.set_dependency_value("item1", "no_such_document", error_if_not_found=False)
        self._assert_rejects(doc, "DID:Database:ValidationDependency")

    def test_non_character_dependency_value(self):
        doc = Document("demoC", **{"demoC.value": 1})
        doc.set_dependency_value("item1", 12345, error_if_not_found=False)
        self._assert_rejects(doc, "DID:Database:ValidationDependNotACharacterArray")

    def test_missing_property_list(self):
        # A demoB doc without the inherited demoA property list fails the
        # superclass recursion.
        doc = Document("demoB", **{"demoB.value": 1, "demoA.value": 1})
        del doc.document_properties["demoA"]
        self._assert_rejects(doc, "DID:Database:PropertyFieldMissing")

    def test_dissimilar_subfields(self):
        doc = Document("demoA", **{"demoA.value": 1})
        doc.document_properties["demoA"]["unexpected"] = 1
        self._assert_rejects(doc, "DID:Database:ValidationFields")

    def test_missing_document_class(self):
        doc = Document("demoA", **{"demoA.value": 1})
        del doc.document_properties["document_class"]
        self._assert_rejects(doc, "DID:Database:MissingRequiredField")


class TestOptionalDependency(_DatabaseTestCase):
    """A depends_on entry with mustbenotempty 0 may be absent.

    demoC declares item1/item2/item3, all optional. Mirrors MATLAB
    TestOptionalDependencyWarning and the reclassified rows of
    TestValidModification.
    """

    def _demo_c_missing_item1(self):
        doc = Document("demoC", **{"demoC.value": 1})
        doc.document_properties["depends_on"] = doc.document_properties["depends_on"][
            1:
        ]
        return doc

    def test_missing_optional_dependency_is_not_an_error(self):
        doc = self._demo_c_missing_item1()
        self.db.add_docs([doc])
        self.assertIsNotNone(
            self.db.get_docs(doc.id()),
            "A document missing an optional dependency should still be added",
        )

    def test_removing_every_optional_dependency_is_valid(self):
        doc = Document("demoC", **{"demoC.value": 1})
        doc.document_properties["depends_on"] = []
        self.db.add_docs([doc])
        self.assertIsNotNone(self.db.get_docs(doc.id()))

    def test_renamed_optional_dependency_is_valid(self):
        # The declared name is then absent (allowed for an optional
        # dependency) and the renamed entry is unrecognized, which validation
        # ignores.
        doc = Document("demoC", **{"demoC.value": 1})
        doc.document_properties["depends_on"][0]["name"] = "invalid name"
        self.db.add_docs([doc])
        self.assertIsNotNone(self.db.get_docs(doc.id()))

    def test_no_warning_when_disabled(self):
        doc = self._demo_c_missing_item1()
        os.environ[ENV_VAR] = "0"
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc])
        self.assertEqual(
            [
                w
                for w in caught
                if issubclass(w.category, MissingOptionalDependencyWarning)
            ],
            [],
            "Missing optional dependency must not warn when the override is disabled",
        )

    def test_no_warning_when_unset(self):
        doc = self._demo_c_missing_item1()
        os.environ.pop(ENV_VAR, None)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc])
        self.assertEqual(
            [
                w
                for w in caught
                if issubclass(w.category, MissingOptionalDependencyWarning)
            ],
            [],
        )

    def test_warning_issued_when_enabled(self):
        doc = self._demo_c_missing_item1()
        os.environ[ENV_VAR] = "1"
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc])
        messages = [
            str(w.message)
            for w in caught
            if issubclass(w.category, MissingOptionalDependencyWarning)
        ]
        self.assertEqual(len(messages), 1)
        self.assertIn("item1", messages[0])
        self.assertIsNotNone(self.db.get_docs(doc.id()))

    def test_warning_forced_through_global_suppression(self):
        # MATLAB forces the warning through a caller's global warning('off')
        # by turning the identifier on for the duration; the Python equivalent
        # is a catch_warnings block with an 'always' filter.
        doc = self._demo_c_missing_item1()
        os.environ[ENV_VAR] = "1"
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("ignore")
            self.db.add_docs([doc])
        self.assertEqual(
            len(
                [
                    w
                    for w in caught
                    if issubclass(w.category, MissingOptionalDependencyWarning)
                ]
            ),
            1,
        )

    def test_required_dependency_still_enforced(self):
        # Flip item1 to required and the same document must now be rejected.
        from did import validate as validate_module

        schema = validate_module.get_document_schema("$DIDSCHEMA_EX1/demoC.schema.json")
        schema["depends_on"][0]["mustbenotempty"] = 1
        doc = self._demo_c_missing_item1()
        with self.assertRaises(ValidationError) as caught:
            validate_module.validate_doc_vs_schema(
                doc.document_properties, schema, [doc.id().lower()]
            )
        self.assertEqual(
            caught.exception.identifier, "DID:Database:ValidationDependsOn"
        )


class TestFieldTypes(unittest.TestCase):
    def test_did_uid(self):
        definition = {"name": "id", "type": "did_uid", "parameters": ""}
        validate_field_type_and_value("doc", "base.id", IDO.unique_id(), definition)
        validate_field_type_and_value("doc", "base.id", "", definition)
        with self.assertRaises(ValidationError):
            validate_field_type_and_value("doc", "base.id", "not-a-uid", definition)

    def test_char_length(self):
        definition = {"name": "name", "type": "char", "parameters": [4]}
        validate_field_type_and_value("doc", "base.name", "abcd", definition)
        with self.assertRaises(ValidationError):
            validate_field_type_and_value("doc", "base.name", "abcde", definition)

    def test_timestamp(self):
        definition = {"name": "datestamp", "type": "timestamp", "parameters": ""}
        validate_field_type_and_value(
            "doc", "base.datestamp", "2018-12-05T18:36:47.241Z", definition
        )
        with self.assertRaises(ValidationError):
            validate_field_type_and_value("doc", "base.datestamp", "nope", definition)

    def test_integer_empty_allowed_only_with_fourth_parameter(self):
        strict = {"name": "v", "type": "integer", "parameters": [0, 10, 0]}
        lenient = {"name": "v", "type": "integer", "parameters": [0, 10, 0, 1]}
        validate_field_type_and_value("doc", "x.v", None, lenient)
        with self.assertRaises(ValidationError):
            validate_field_type_and_value("doc", "x.v", None, strict)

    def test_double_accepts_non_integral(self):
        definition = {"name": "v", "type": "double", "parameters": [0, 10, 0]}
        validate_field_type_and_value("doc", "x.v", 1.5, definition)
        with self.assertRaises(ValidationError):
            validate_field_type_and_value("doc", "x.v", 99.0, definition)

    def test_matrix_shape(self):
        definition = {"name": "m", "type": "matrix", "parameters": [2, 2]}
        validate_field_type_and_value("doc", "x.m", [[1, 2], [3, 4]], definition)
        with self.assertRaises(ValidationError):
            validate_field_type_and_value("doc", "x.m", [[1, 2, 3]], definition)

    def test_structure_and_cell(self):
        validate_field_type_and_value(
            "doc", "x.s", {"a": 1}, {"name": "s", "type": "structure", "parameters": ""}
        )
        validate_field_type_and_value(
            "doc", "x.c", [1, 2], {"name": "c", "type": "cell", "parameters": ""}
        )
        with self.assertRaises(ValidationError):
            validate_field_type_and_value(
                "doc", "x.s", 5, {"name": "s", "type": "structure", "parameters": ""}
            )

    def test_unknown_type_is_rejected(self):
        with self.assertRaises(ValidationError):
            validate_field_type_and_value(
                "doc", "x.v", 1, {"name": "v", "type": "bogus", "parameters": ""}
            )


class TestFilenameMatching(unittest.TestCase):
    """Mirrors MATLAB database.isfilenamematch."""

    def test_exact_match(self):
        self.assertTrue(is_filename_match("a.ext", "a.ext"))
        self.assertFalse(is_filename_match("a.ext", "b.ext"))

    def test_enumerated_match(self):
        self.assertTrue(is_filename_match("frame_#", "frame_12"))
        self.assertFalse(is_filename_match("frame_#", "frame_x"))
        self.assertFalse(is_filename_match("frame_#", "frame_"))


class TestDocumentIdentifiers(unittest.TestCase):
    """IDs must match MATLAB's did.ido format, which the base schema requires."""

    def test_generated_id_is_valid(self):
        self.assertTrue(IDO.is_valid(IDO.unique_id()))

    def test_id_shape(self):
        generated = IDO.unique_id()
        self.assertEqual(len(generated), 33)
        self.assertEqual(generated[16], "_")

    def test_rejects_uuid(self):
        self.assertFalse(IDO.is_valid("3d9f16fd-784e-45ef-b62b-0d920bff9dad"))

    def test_ids_sort_by_creation_time(self):
        ids = [IDO.unique_id() for _ in range(5)]
        self.assertEqual(
            [i.split("_")[0] for i in ids], sorted(i.split("_")[0] for i in ids)
        )


if __name__ == "__main__":
    unittest.main()
