"""isa-operator parity: the brute-force (field_search) and SQL search paths must
agree, and both must follow MATLAB's isa semantics -- a document is isa(X) iff X
is its class OR one of its superclasses (matched by bare name, as produced by
did.implementations.doc2sql's meta.class / meta.superclass).

Regression for the DID-python isa divergence (audit 6.1-1): the old brute-force
field_search isa used a ``param1 in a`` heuristic plus an exact class_name
check, so it (a) missed superclass membership -- a probe did not match
isa('element') -- and (b) spuriously matched a class name that happened to be an
incidental top-level field of an unrelated document. Same query, different
result set per language. The SQL path already matched MATLAB (meta.class /
meta.superclass); this test pins both paths to the same, correct answer.
"""

import os
import unittest

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query
from did.datastructures import field_search


def _doc(class_name, superclasses, fields=None):
    props = {
        "document_class": {
            "definition": f"$NDIDOCUMENTPATH/{class_name}.json",
            "class_name": class_name,
            "class_version": 1,
            "property_list_name": class_name,
            "superclasses": [
                {"definition": f"$NDIDOCUMENTPATH/{s}.json"} for s in superclasses
            ],
        },
        "base": {
            "id": f"id_{class_name}",
            "name": class_name,
            "datestamp": "2026-06-12T00:00:00",
        },
    }
    if fields:
        props.update(fields)
    return Document(props)


class TestIsaParity(unittest.TestCase):
    DB = "test_isa_parity.sqlite"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.DB):
            os.remove(cls.DB)
        cls.db = SQLiteDB(cls.DB)
        cls.db.add_branch("a")
        # probe isa {probe, element, base}; element isa {element, base};
        # session isa {session, base}. 'thing' is class thing with superclass
        # base only, but carries an incidental top-level 'element' field -- it
        # must NOT match isa('element') (the old heuristic wrongly would).
        specs = [
            ("probe", ["element", "base"], {"probe": {"name": "p"}}),
            ("element", ["base"], {"element": {"name": "e"}}),
            ("session", ["base"], {"session": {"name": "s"}}),
            ("thing", ["base"], {"element": {"note": "incidental, not a superclass"}}),
        ]
        cls.by_class = {}
        cls.docs = []
        for class_name, superclasses, fields in specs:
            d = _doc(class_name, superclasses, fields)
            cls.by_class[class_name] = d
            cls.docs.append(d)
            cls.db._do_add_doc(d, "a")

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        if os.path.exists(cls.DB):
            os.remove(cls.DB)

    def _brute(self, class_name):
        ss = Query("", "isa", class_name).to_search_structure()
        return sorted(
            d.id() for d in self.docs if field_search(d.document_properties, ss)
        )

    def _sql(self, class_name):
        return sorted(self.db.search(Query("", "isa", class_name), branch_id="a"))

    def _expect(self, *class_names):
        return sorted(self.by_class[c].id() for c in class_names)

    def test_isa_own_class(self):
        self.assertEqual(self._sql("probe"), self._expect("probe"))
        self.assertEqual(self._brute("probe"), self._expect("probe"))

    def test_isa_superclass_matches_descendant(self):
        # isa(element) -> the probe (element is its superclass) AND element.
        # Must NOT include 'thing' despite its incidental 'element' field.
        self.assertEqual(self._sql("element"), self._expect("probe", "element"))
        self.assertEqual(self._brute("element"), self._expect("probe", "element"))

    def test_isa_root_superclass_matches_all(self):
        self.assertEqual(
            self._sql("base"), self._expect("probe", "element", "session", "thing")
        )
        self.assertEqual(
            self._brute("base"), self._expect("probe", "element", "session", "thing")
        )

    def test_isa_unrelated_matches_nothing(self):
        self.assertEqual(self._sql("nonexistent"), [])
        self.assertEqual(self._brute("nonexistent"), [])

    def test_sql_and_brute_force_agree(self):
        for c in ["probe", "element", "base", "session", "thing", "nonexistent"]:
            self.assertEqual(
                self._sql(c), self._brute(c), f"isa({c}) SQL vs brute-force mismatch"
            )


class TestSqlFieldNameValidation(unittest.TestCase):
    """A query field name with SQL metacharacters must not be interpolated into
    SQL; the leaf falls back to the injection-free brute-force path (returns the
    correct empty set here rather than raising or injecting)."""

    DB = "test_isa_fieldname.sqlite"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.DB):
            os.remove(cls.DB)
        cls.db = SQLiteDB(cls.DB)
        cls.db.add_branch("a")
        cls.db._do_add_doc(_doc("probe", ["base"], {"probe": {"name": "p"}}), "a")

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        if os.path.exists(cls.DB):
            os.remove(cls.DB)

    def test_malicious_field_name_does_not_inject(self):
        # Classic injection attempt as the field name.
        evil = "base.id' OR '1'='1"
        # Must not raise, must not return everything via injection.
        result = self.db.search(Query(evil, "exact_string", "anything"), branch_id="a")
        self.assertEqual(result, [])

    def test_legitimate_dotted_field_still_works(self):
        result = self.db.search(
            Query("base.name", "exact_string", "probe"), branch_id="a"
        )
        self.assertEqual(result, ["id_probe"])


class TestVEpsilonDiamondIsa(unittest.TestCase):
    """V_epsilon's observation tier is the first MULTIPLE-INHERITANCE (diamond)
    hierarchy: ``body_weight_observation`` isa ``scalar_observation`` AND
    ``scalar_mass``, both reaching ``base``. A produced V_epsilon document
    carries its FLATTENED ancestor list as bare ``{class_name}`` entries. Both
    isa paths -- the SQL ``meta.superclass`` column (populated by
    ``doc2sql._get_superclass_str``) and the brute-force ``field_search`` -- must
    resolve every ancestor, reached via either parent, with no spurious match.
    """

    DB = "test_isa_v_epsilon_diamond.sqlite"

    @staticmethod
    def _v2doc(class_name, superclasses):
        # V_epsilon shape: superclasses named by bare {class_name} (not {definition}).
        return Document(
            {
                "document_class": {
                    "definition": f"$NDIDOCUMENTPATH/{class_name}.json",
                    "class_name": class_name,
                    "class_version": 1,
                    "property_list_name": class_name,
                    "superclasses": [{"class_name": s} for s in superclasses],
                },
                "base": {
                    "id": f"id_{class_name}",
                    "name": class_name,
                    "datestamp": "2026-06-24T00:00:00",
                },
            }
        )

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.DB):
            os.remove(cls.DB)
        cls.db = SQLiteDB(cls.DB)
        cls.db.add_branch("a")
        # The diamond leaf carries its FLATTENED ancestor list, exactly as a real
        # producer stamps it (both parents + the shared root).
        specs = [
            ("base", []),
            ("scalar_observation", ["base"]),
            ("scalar_mass", ["base"]),
            ("body_weight_observation", ["scalar_observation", "scalar_mass", "base"]),
        ]
        cls.by_class = {}
        cls.docs = []
        for class_name, supers in specs:
            d = cls._v2doc(class_name, supers)
            cls.by_class[class_name] = d
            cls.docs.append(d)
            cls.db._do_add_doc(d, "a")

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        if os.path.exists(cls.DB):
            os.remove(cls.DB)

    def _sql(self, class_name):
        return sorted(self.db.search(Query("", "isa", class_name), branch_id="a"))

    def _brute(self, class_name):
        ss = Query("", "isa", class_name).to_search_structure()
        return sorted(
            d.id() for d in self.docs if field_search(d.document_properties, ss)
        )

    def _expect(self, *class_names):
        return sorted(self.by_class[c].id() for c in class_names)

    def test_diamond_leaf_isa_both_parents_and_shared_ancestor(self):
        leaf = "id_body_weight_observation"
        for ancestor in ("scalar_observation", "scalar_mass", "base"):
            self.assertIn(
                leaf, self._sql(ancestor), f"SQL: leaf must be isa({ancestor})"
            )
            self.assertIn(leaf, self._brute(ancestor), f"brute: leaf isa({ancestor})")

    def test_base_matches_whole_diamond(self):
        # base is the root: every class in the diamond is isa(base).
        self.assertEqual(
            self._sql("base"),
            self._expect(
                "base", "scalar_observation", "scalar_mass", "body_weight_observation"
            ),
        )

    def test_diamond_sql_and_brute_agree(self):
        for c in (
            "body_weight_observation",
            "scalar_observation",
            "scalar_mass",
            "base",
            "nonexistent",
        ):
            self.assertEqual(
                self._sql(c), self._brute(c), f"isa({c}) SQL vs brute-force mismatch"
            )


if __name__ == "__main__":
    unittest.main()
