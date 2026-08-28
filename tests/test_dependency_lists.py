"""Enumerated dependency lists: name_1, name_2, ...

Mirrors MATLAB's did.document/dependency_value_n, add_dependency_value_n and
remove_dependency_value_n, which have no MATLAB tests of their own. The
regression these guard against is silent: before the _n methods existed, the
only way to append to such a list wrote a stem-named entry that both
validators accept and MATLAB's reader cannot see.
"""

import pytest

from did.document import Document


def make_doc(names_and_values):
    doc = Document("demoC")
    doc.document_properties["depends_on"] = [
        {"name": name, "value": value} for name, value in names_and_values
    ]
    return doc


def names(doc):
    return [d["name"] for d in doc.document_properties["depends_on"]]


def values(doc):
    return [d["value"] for d in doc.document_properties["depends_on"]]


class TestDependencyValueN:
    def test_returns_values_in_order(self):
        doc = make_doc([("item_1", "a"), ("item_2", "b"), ("item_3", "c")])
        assert doc.dependency_value_n("item") == ["a", "b", "c"]

    def test_stops_at_the_first_gap(self):
        # item_4 sits above a gap, so MATLAB never sees it -- and neither do we.
        doc = make_doc([("item_1", "a"), ("item_2", "b"), ("item_4", "d")])
        assert doc.dependency_value_n("item") == ["a", "b"]

    def test_raises_when_absent(self):
        doc = make_doc([("other_1", "a")])
        with pytest.raises(ValueError, match="not found"):
            doc.dependency_value_n("item")

    def test_returns_empty_when_absent_and_not_erroring(self):
        doc = make_doc([("other_1", "a")])
        assert doc.dependency_value_n("item", error_if_not_found=False) == []

    def test_is_case_insensitive_like_matlab_strcmpi(self):
        doc = make_doc([("Item_1", "a"), ("ITEM_2", "b")])
        assert doc.dependency_value_n("item") == ["a", "b"]

    def test_does_not_confuse_a_plain_name_for_an_enumerated_one(self):
        doc = make_doc([("item", "plain")])
        assert doc.dependency_value_n("item", error_if_not_found=False) == []
        assert doc.dependency_value("item") == "plain"


class TestAddDependencyValueN:
    def test_appends_the_next_index(self):
        doc = make_doc([("item_1", "a"), ("item_2", "b")])
        doc.add_dependency_value_n("item", "c")
        assert names(doc) == ["item_1", "item_2", "item_3"]
        assert doc.dependency_value_n("item") == ["a", "b", "c"]

    def test_starts_at_one_on_an_empty_list(self):
        doc = make_doc([("other", "x")])
        doc.add_dependency_value_n("item", "a")
        assert names(doc) == ["other", "item_1"]

    def test_numbers_from_the_contiguous_count_not_the_highest_suffix(self):
        # item_4 is above a gap and invisible to MATLAB, so the next append
        # takes index 3, exactly as MATLAB's numel(d)+1 does.
        doc = make_doc([("item_1", "a"), ("item_2", "b"), ("item_4", "d")])
        doc.add_dependency_value_n("item", "c")
        assert names(doc) == ["item_1", "item_2", "item_4", "item_3"]
        assert doc.dependency_value_n("item") == ["a", "b", "c", "d"]

    def test_raises_on_a_document_with_no_dependencies(self):
        doc = Document("demoC")
        doc.document_properties.pop("depends_on", None)
        with pytest.raises(ValueError, match="does not have any dependencies"):
            doc.add_dependency_value_n("item", "a")

    def test_adds_anyway_when_not_erroring(self):
        doc = Document("demoC")
        doc.document_properties.pop("depends_on", None)
        doc.add_dependency_value_n("item", "a", error_if_not_found=False)
        assert names(doc) == ["item_1"]


class TestRemoveDependencyValueN:
    def test_removes_and_renumbers(self):
        doc = make_doc([("item_1", "a"), ("item_2", "b"), ("item_3", "c")])
        doc.remove_dependency_value_n("item", None, 2)
        assert names(doc) == ["item_1", "item_2"]
        assert values(doc) == ["a", "c"]

    def test_removing_the_last_leaves_no_gap(self):
        doc = make_doc([("item_1", "a"), ("item_2", "b")])
        doc.remove_dependency_value_n("item", None, 2)
        assert names(doc) == ["item_1"]

    def test_removing_the_first_renumbers_everything_above(self):
        doc = make_doc([("item_1", "a"), ("item_2", "b"), ("item_3", "c")])
        doc.remove_dependency_value_n("item", None, 1)
        assert names(doc) == ["item_1", "item_2"]
        assert values(doc) == ["b", "c"]

    def test_leaves_unrelated_dependencies_alone(self):
        doc = make_doc([("other", "x"), ("item_1", "a"), ("item_2", "b")])
        doc.remove_dependency_value_n("item", None, 1)
        assert names(doc) == ["other", "item_1"]
        assert values(doc) == ["x", "b"]

    def test_raises_when_n_exceeds_the_count(self):
        doc = make_doc([("item_1", "a")])
        with pytest.raises(ValueError, match="greater than total number"):
            doc.remove_dependency_value_n("item", None, 5)

    def test_raises_when_the_entry_is_missing_even_when_not_erroring(self):
        # MATLAB's "Could not locate entry" check is unconditional.
        doc = make_doc([("item_1", "a")])
        with pytest.raises(ValueError, match="Could not locate entry"):
            doc.remove_dependency_value_n("item", None, 5, error_if_not_found=False)

    def test_round_trips_with_add(self):
        doc = make_doc([("item_1", "a"), ("item_2", "b"), ("item_3", "c")])
        doc.remove_dependency_value_n("item", None, 2)
        doc.add_dependency_value_n("item", "d")
        assert doc.dependency_value_n("item") == ["a", "c", "d"]


class TestSetDependencyValueGuard:
    """The regression that motivated the port."""

    def test_refuses_to_append_a_stem_name_beside_an_enumerated_list(self):
        doc = make_doc([("item_1", "a"), ("item_2", "b"), ("item_3", "c")])
        with pytest.raises(ValueError, match="add_dependency_value_n"):
            doc.set_dependency_value("item", "d", error_if_not_found=False)
        assert names(doc) == ["item_1", "item_2", "item_3"]

    def test_still_updates_an_existing_enumerated_entry(self):
        doc = make_doc([("item_1", "a"), ("item_2", "b")])
        doc.set_dependency_value("item_2", "updated")
        assert doc.dependency_value_n("item") == ["a", "updated"]

    def test_still_updates_an_existing_plain_entry(self):
        doc = make_doc([("item", "a")])
        doc.set_dependency_value("item", "updated", error_if_not_found=False)
        assert values(doc) == ["updated"]

    def test_still_appends_when_no_enumerated_siblings_exist(self):
        doc = make_doc([("other", "x")])
        doc.set_dependency_value("item", "a", error_if_not_found=False)
        assert names(doc) == ["other", "item"]

    def test_matching_is_case_insensitive(self):
        doc = make_doc([("Item1", "a")])
        assert doc.dependency_value("item1") == "a"
        doc.set_dependency_value("ITEM1", "updated")
        assert values(doc) == ["updated"]
