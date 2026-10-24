"""'hasmember': one rule in field_search and in SQL (DID-matlab#236).

DID-matlab's SQL search put a text 'hasmember' value into the SQL unquoted, so
'value' matched every document and '1 OR 1=1' was SQL injection. Python never
builds SQL for hasmember -- _query_struct_to_sql_str returns None, and the term
is brute-forced with field_search -- so it was never affected. But DID-matlab's
fieldsearch (ismember, which compares two texts character by character), its
SQL search and field_search (``in``: a substring test) each matched another
rule. All three now match one:

- text is a member of text that is the whole value or an item of its
  comma-separated list;
- anything is a member of a list that holds an equal element, and a number
  of an equal number;
- text is never a member of a number, nor a number of text.

test_rule checks field_search against the table of DID-matlab's
did.unittest.TestHasMemberQuoting/testRule. The other tests pin the cases of
that class: text values (including 'value', '1 OR 1=1' and values holding "
and '), numbers and ~hasmember, alone, inside an AND and inside an OR,
compared with field_search and with the documents DID-matlab finds.
"""

import os
import tempfile
import unittest
import uuid

from did.datastructures import field_search
from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query

NAMES = ["value", "1 OR 1=1", 'say "hi"', "x, value", "alpha", "n7", "O'Brien", "5"]


# The documents DID-matlab finds for each text value
TEXT_CASES = {
    "value": ("value", "x, value"),
    "1 OR 1=1": ("1 OR 1=1",),
    'say "hi"': ('say "hi"',),
    "O'Brien": ("O'Brien",),
    "x, value": ("x, value",),
    "name17": (),
    "zzz": (),
    "5": ("5",),
    "": (),
}

# (value, member, is a member): DID-matlab's TestHasMemberQuoting/testRule
RULE_CASES = [
    ("value", "value", True),
    ("x, value", "value", True),
    ("x,value ,y", "value", True),
    ("valuex", "value", False),
    ("value", "x, value", False),
    ("eulav", "value", False),  # MATLAB's ismember matched this
    ("a.b", ".", False),  # the member is not a pattern
    ("", "", True),
    (5, 5, True),
    ([1, 2, 3], 2, True),
    ([1, 2, 3], 4, False),
    (True, 1, True),
    (["a", "b"], "b", True),
    (["a", "b"], "a,b", False),
    ([1, "a"], 1, True),
    ("5", 5, False),
    (5, "5", False),
    ("a", 97, False),  # MATLAB's ismember matched this
    ({"a": 1}, "a", False),
]


def _doc(name, value):
    return Document(
        {
            "base": {
                "id": uuid.uuid4().hex,
                "session_id": "s1",
                "name": name,
                "datestamp": "2026-10-24T00:00:00.000Z",
            },
            "document_class": {
                "class_name": "demoA",
                "superclasses": [{"class_name": "base"}],
            },
            "depends_on": [],
            "demoA": {"value": value},
        }
    )


def _has_member(field, value, negate=False):
    return Query(field, ("~" if negate else "") + "hasmember", value)


def _or_of(queries):
    q = queries[0]
    for other in queries[1:]:
        q = q | other
    return q


class TestHasMemberQuoting(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.db = SQLiteDB(os.path.join(cls.tmpdir.name, "hasmember.sqlite"))
        cls.db.add_branch("a")
        cls.docs = [_doc(name, i) for i, name in enumerate(NAMES, 1)]
        cls.ids = [d.id() for d in cls.docs]
        cls.db.add_docs(cls.docs, branch_id="a", validate=False)

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        cls.tmpdir.cleanup()

    def expected(self, query):
        s = query.to_search_structure()
        return sorted(
            d.id() for d in self.docs if field_search(d.document_properties, s)
        )

    def verify_search(self, query, ids, msg):
        found = sorted(self.db.search(query))
        self.assertEqual(found, self.expected(query), f"{msg}: vs. field_search")
        self.assertEqual(found, sorted(ids), f"{msg}: vs. DID-matlab")

    def names_matching(self, *names):
        return [self.ids[NAMES.index(n)] for n in names]

    def complement(self, ids):
        return [i for i in self.ids if i not in ids]

    def test_rule(self):
        for k, (value, member, is_member) in enumerate(RULE_CASES, 1):
            with self.subTest(case=k):
                a = {"f": value}
                s = {"field": "f", "operation": "hasmember", "param1": member}
                self.assertEqual(field_search(a, s), is_member)
                s["operation"] = "~hasmember"
                self.assertEqual(field_search(a, s), not is_member)

    def test_text_and_numbers_do_not_mix(self):
        # base.name "5" is text; demoA.value 3 is a number
        self.verify_search(_has_member("base.name", 5), [], "hasmember 5 in '5'")
        self.verify_search(
            _has_member("base.name", "5"), self.names_matching("5"), "'5' in '5'"
        )
        self.verify_search(_has_member("demoA.value", "3"), [], "'3' in 3")
        self.verify_search(_has_member("demoA.value", 3), [self.ids[2]], "3 in 3")

    def test_numbers_no_value_holds(self):
        # DID-matlab writes Inf as 9e999 and makes NaN a member of nothing
        for member in (float("inf"), float("-inf"), float("nan")):
            with self.subTest(member=member):
                q = _has_member("demoA.value", member)
                self.verify_search(q, [], f"hasmember {member}")
                q = _has_member("demoA.value", member, True)
                self.verify_search(q, self.ids, f"~hasmember {member}")

    def test_struct_field(self):
        # A struct (base) has no members: its fields are not searched
        self.verify_search(_has_member("base", "value"), [], "'value' in base")

    def test_text(self):
        for value, names in TEXT_CASES.items():
            ids = self.names_matching(*names)
            with self.subTest(value=value):
                self.verify_search(
                    _has_member("base.name", value), ids, f"hasmember {value!r}"
                )
                self.verify_search(
                    _has_member("base.name", value, True),
                    self.complement(ids),
                    f"~hasmember {value!r}",
                )

    def test_number(self):
        for value in (3, 5, 99):
            ids = [self.ids[value - 1]] if value <= len(NAMES) else []
            with self.subTest(value=value):
                self.verify_search(
                    _has_member("demoA.value", value), ids, f"hasmember {value}"
                )
                self.verify_search(
                    _has_member("demoA.value", value, True),
                    self.complement(ids),
                    f"~hasmember {value}",
                )

    def test_in_an_and(self):
        every_doc = Query("demoA.value", "lessthan", 100)
        for value, names in TEXT_CASES.items():
            ids = self.names_matching(*names)
            with self.subTest(value=value):
                self.verify_search(
                    every_doc & _has_member("base.name", value),
                    ids,
                    f"... AND hasmember {value!r}",
                )
                self.verify_search(
                    every_doc & _has_member("base.name", value, True),
                    self.complement(ids),
                    f"... AND ~hasmember {value!r}",
                )
        self.verify_search(
            every_doc & _has_member("demoA.value", 5), [self.ids[4]], "... AND 5"
        )

    def test_in_an_or(self):
        terms = [
            (
                _has_member("base.name", "value"),
                self.names_matching("value", "x, value"),
            ),
            (_has_member("base.name", "1 OR 1=1"), self.names_matching("1 OR 1=1")),
            (_has_member("base.name", 'say "hi"'), self.names_matching('say "hi"')),
            (
                _has_member("base.name", "O'Brien", True),
                self.complement(self.names_matching("O'Brien")),
            ),
            (_has_member("demoA.value", 6), [self.ids[5]]),
            (_has_member("demoA.value", 2, True), self.complement([self.ids[1]])),
        ]
        alpha = Query("base.name", "exact_string", "alpha")
        for k, (q, ids) in enumerate(terms):
            other, other_ids = terms[(k + 1) % len(terms)]
            with self.subTest(term=k):
                self.verify_search(
                    q | alpha,
                    sorted(set(ids) | set(self.names_matching("alpha"))),
                    "... OR alpha",
                )
                self.verify_search(
                    q | other, sorted(set(ids) | set(other_ids)), "... OR hasmember"
                )
        everything = sorted(set().union(*(ids for _, ids in terms)))
        self.verify_search(_or_of([q for q, _ in terms]), everything, "an OR of all")
        self.verify_search(
            Query("demoA.value", "lessthan", 100) & _or_of([q for q, _ in terms]),
            everything,
            "... AND an OR of all",
        )


if __name__ == "__main__":
    unittest.main()
