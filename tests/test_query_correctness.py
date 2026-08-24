"""Query-correctness tests for the SQLite leaf-query builder.

These cover three narrowing/robustness fixes in
``SQLiteDB._query_struct_to_sql_str`` / ``_search_doc_ids``:

* ``hasfield`` must not let an underscore in the field name act as a LIKE
  single-character wildcard (e.g. querying ``a_b`` must not match ``axb``).
* ``isa`` must not let a regex metacharacter in the class name act as a
  wildcard in the ``meta.superclass`` regexp (e.g. ``foo.bar`` must not match
  ``fooxbar``).
* A numeric operation given a non-numeric ``param1`` must fall back to
  brute-force search instead of raising and aborting the whole query.
"""

import os
import unittest

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query


def _make_doc(idhex, *, fields=None, class_name=None, superclasses=None):
    """Build a Document with explicit flattened fields / class metadata.

    ``fields`` is a mapping of dotted field name -> value placed verbatim into
    the document properties so that ``doc2sql`` stores those exact field names.
    ``superclasses`` (a list of plain strings) populates ``meta.superclass`` as
    a comma-space joined list without any path stripping.
    """
    props = {
        "base": {"id": idhex, "datestamp": "2020-01-01"},
        "document_class": {"class_name": class_name or "thing"},
    }
    if superclasses is not None:
        props["superclasses"] = list(superclasses)
    if fields:
        for dotted, value in fields.items():
            group, _, leaf = dotted.partition(".")
            props.setdefault(group, {})
            if leaf:
                props[group][leaf] = value
            else:
                props[group] = value
    return Document(props)


class TestQueryCorrectness(unittest.TestCase):
    DB_FILENAME = "test_query_correctness.sqlite"

    def setUp(self):
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)
        self.db = SQLiteDB(self.DB_FILENAME)
        self.db.add_branch("a")

    def tearDown(self):
        self.db._close_db()
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)

    def _add(self, doc):
        self.db._do_add_doc(doc, "a")
        return doc.id()

    # --- hasfield: '_' must not behave as a LIKE wildcard ---------------------

    def test_hasfield_underscore_is_not_a_wildcard(self):
        # Document whose subfield lives under the underscored group 'a_b'.
        match_id = self._add(_make_doc("1" * 32, fields={"a_b.value": 1}))
        # Decoy whose group 'axb' differs only at the position of the '_'.
        # With an unescaped LIKE pattern ('a_b.%'), '_' matches the 'x' here and
        # this document would be a false positive.
        self._add(_make_doc("2" * 32, fields={"axb.value": 2}))

        ids = self.db.search(Query("a_b", "hasfield"), branch_id="a")

        self.assertEqual(ids, [match_id])

    def test_hasfield_exact_underscored_field(self):
        # The exact (non-subfield) branch must still match an underscored field.
        match_id = self._add(_make_doc("3" * 32, fields={"a_b.value": 1}))
        self._add(_make_doc("4" * 32, fields={"axb.value": 2}))

        ids = self.db.search(Query("a_b.value", "hasfield"), branch_id="a")

        self.assertEqual(ids, [match_id])

    # --- isa: regex metacharacters in the class name must be escaped ----------

    def test_isa_dot_is_not_a_regex_wildcard(self):
        # 'meta.superclass' for this doc holds two list elements that differ
        # only at the position of the '.': 'foo.bar' and 'fooxbar'.
        match_id = self._add(_make_doc("5" * 32, superclasses=["foo.bar", "fooxbar"]))
        # A doc carrying only the decoy superclass must never match 'foo.bar'.
        self._add(_make_doc("6" * 32, superclasses=["fooxbar"]))

        ids = self.db.search(Query("", "isa", "foo.bar"), branch_id="a")

        self.assertEqual(ids, [match_id])

    def test_isa_dot_class_does_not_match_decoy_only(self):
        # A document whose only superclass is the decoy 'fooxbar' must produce
        # the empty result for an isa('foo.bar') query.
        self._add(_make_doc("7" * 32, superclasses=["fooxbar"]))

        ids = self.db.search(Query("", "isa", "foo.bar"), branch_id="a")

        self.assertEqual(ids, [])

    # --- contains_string: '_'/'%' must not behave as LIKE wildcards -----------

    def test_contains_string_underscore_is_not_a_wildcard(self):
        # Two docs whose base.name differs only at the position of the '_'.
        # With an unescaped LIKE pattern ('%spike_sort%'), '_' matches the 'X'
        # here and the decoy would be a false positive, diverging from the
        # brute-force ``param1 in value`` path.
        match_id = self._add(_make_doc("1" * 32, fields={"base.name": "spike_sort"}))
        self._add(_make_doc("2" * 32, fields={"base.name": "spikeXsort"}))

        ids = self.db.search(
            Query("base.name", "contains_string", "spike_sort"), branch_id="a"
        )

        # SQL result must agree with the brute-force path: exactly one match.
        self.assertEqual(ids, [match_id])

    def test_contains_string_percent_is_not_a_wildcard(self):
        # '%' in the search term must match a literal '%', not "any run".
        match_id = self._add(_make_doc("3" * 32, fields={"base.name": "50%done"}))
        self._add(_make_doc("4" * 32, fields={"base.name": "50something_elsedone"}))

        ids = self.db.search(
            Query("base.name", "contains_string", "50%done"), branch_id="a"
        )

        self.assertEqual(ids, [match_id])

    def test_depends_on_like_pattern_escapes_wildcards(self):
        # The (currently unreachable) SQL depends_on branch must LIKE-escape the
        # dependency name so a '_' in it cannot act as a single-char wildcard.
        struct = {
            "field": "",
            "operation": "depends_on",
            "param1": "a_b",
            "param2": "val",
        }
        sql = self.db._query_struct_to_sql_str(struct)

        # The underscore is escaped and an ESCAPE clause is emitted.
        self.assertIn("a\\_b", sql)
        self.assertIn("ESCAPE", sql)

    # --- numeric op with a non-numeric param1: graceful fallback --------------

    def test_numeric_op_nonnumeric_param_does_not_raise(self):
        # A genuinely numeric field plus a non-numeric comparison value used to
        # raise ValueError from float() and abort the entire search. It must now
        # fall back to brute force and simply return no matches.
        self._add(_make_doc("8" * 32, fields={"demoA.value": 5}))

        ids = self.db.search(
            Query("demoA.value", "lessthan", "not-a-number"), branch_id="a"
        )

        self.assertEqual(ids, [])

    def test_numeric_op_nonnumeric_param_in_compound_query(self):
        # The bad numeric leaf must not poison an OR'd query: the search should
        # complete and return the matches from the well-formed branch.
        good_id = self._add(_make_doc("9" * 32, fields={"demoA.value": 5}))

        q = Query("demoA.value", "exact_number", "oops") | Query(
            "demoA.value", "exact_number", 5
        )
        ids = self.db.search(q, branch_id="a")

        self.assertEqual(ids, [good_id])


if __name__ == "__main__":
    unittest.main()
