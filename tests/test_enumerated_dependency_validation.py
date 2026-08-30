"""An enumerated dependency must have its VALUE validated, not just its name.

`_validate_depends_on` strips the enumeration suffix into ``doc_names_alt``
and uses that for the presence check, but the loop reading each value back
used to match on the un-stripped names. The schema declares ``item1`` while
the document holds ``item1_1``, so the two never matched: ``value`` stayed
None, ``_is_empty(None)`` was true, and both the ``mustbenotempty`` check and
the dependent-id resolution check were skipped entirely. A dependency could
point at a document in no database and in no batch and still validate.

Shared with DID-matlab (VH-Lab/DID-python#41); the same shape existed there
and the fix landed in `+did/database.m` first. The MATLAB mirror of this file
is `tests/+did/+unittest/TestEnumeratedDependencyValidation.m`.
"""

import os
import tempfile
import unittest

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.validate import ValidationError

DANGLING = "0" * 16 + "_" + "0" * 16


class TestEnumeratedDependencyValidation(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "t.sqlite"))
        self.db.add_branch("a")
        self.db.set_branch("a")

    def tearDown(self):
        self.db._close_db()

    def _demo_c(self, depends_on):
        """A demoC document carrying exactly these depends_on entries.

        demoC's three dependencies (item1/item2/item3) are all declared
        mustbenotempty:0, so presence is never what is under test here -- the
        value is.
        """
        doc = Document("demoC", **{"demoC.value": 1})
        doc.document_properties["depends_on"] = depends_on
        return doc

    def _demo_a(self):
        return Document("demoA", **{"demoA.value": 1})

    def test_an_enumerated_dependency_with_a_dangling_value_is_rejected(self):
        """The headline bug: this used to validate cleanly."""
        doc = self._demo_c([{"name": "item1_1", "value": DANGLING}])

        with self.assertRaises(ValidationError) as caught:
            self.db.add_docs([doc])
        self.assertEqual(
            caught.exception.identifier, "DID:Database:ValidationDependency"
        )

    def test_every_enumerated_entry_is_checked_not_just_the_first(self):
        """The old lookup stopped at the first match."""
        good = self._demo_a()
        self.db.add_docs([good])

        doc = self._demo_c(
            [
                {"name": "item1_1", "value": good.id()},
                {"name": "item1_2", "value": DANGLING},
            ]
        )

        with self.assertRaises(ValidationError) as caught:
            self.db.add_docs([doc])
        self.assertEqual(
            caught.exception.identifier, "DID:Database:ValidationDependency"
        )

    def test_the_failing_entry_is_named_in_the_message(self):
        """'item1_2', not the schema's stem 'item1'."""
        good = self._demo_a()
        self.db.add_docs([good])

        doc = self._demo_c(
            [
                {"name": "item1_1", "value": good.id()},
                {"name": "item1_2", "value": DANGLING},
            ]
        )

        with self.assertRaises(ValidationError) as caught:
            self.db.add_docs([doc])
        self.assertIn("item1_2", str(caught.exception))

    def test_an_enumerated_dependency_resolving_in_the_database_is_accepted(self):
        good = self._demo_a()
        self.db.add_docs([good])

        doc = self._demo_c([{"name": "item1_1", "value": good.id()}])
        self.db.add_docs([doc])

        self.assertIsNotNone(self.db.get_docs(doc.id(), OnMissing="ignore"))

    def test_an_enumerated_dependency_resolving_within_the_batch_is_accepted(self):
        """all_ids covers the documents being added alongside, not just stored."""
        good = self._demo_a()
        doc = self._demo_c([{"name": "item1_1", "value": good.id()}])

        self.db.add_docs([good, doc])

        self.assertIsNotNone(self.db.get_docs(doc.id(), OnMissing="ignore"))

    def test_an_unenumerated_dependency_still_validates(self):
        """The un-enumerated path was already correct; keep it that way."""
        doc = self._demo_c([{"name": "item1", "value": DANGLING}])

        with self.assertRaises(ValidationError) as caught:
            self.db.add_docs([doc])
        self.assertEqual(
            caught.exception.identifier, "DID:Database:ValidationDependency"
        )

    def test_an_empty_enumerated_dependency_is_allowed_when_optional(self):
        """demoC's dependencies are optional: an empty value has nothing to resolve."""
        doc = self._demo_c([{"name": "item1_1", "value": ""}])

        self.db.add_docs([doc])

        self.assertIsNotNone(self.db.get_docs(doc.id(), OnMissing="ignore"))

    def test_a_non_string_enumerated_value_is_rejected(self):
        doc = self._demo_c([{"name": "item1_1", "value": 42}])

        with self.assertRaises(ValidationError) as caught:
            self.db.add_docs([doc])
        self.assertEqual(
            caught.exception.identifier,
            "DID:Database:ValidationDependNotACharacterArray",
        )


if __name__ == "__main__":
    unittest.main()
