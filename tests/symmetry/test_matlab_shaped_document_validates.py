"""Would Python's validator accept a document MATLAB wrote?

The readArtifacts suites now run each language's validator over the other
language's documents (DID-python#28 / DID-matlab#155). That gate only runs
where both languages are installed, which is the symmetry CI job -- there is
no MATLAB here.

So this test builds documents in the shape MATLAB writes them, by hand, and
holds Python's validator to accepting them. It is a proxy for the real gate,
not a replacement: it cannot notice a MATLAB output shape nobody thought to
write down here. What it does catch is a Python-side validation rule that
would reject MATLAB's formats -- the class of bug #28 exists to surface --
without waiting for a MATLAB runner.

The formats that differ, or once did:

* ``base.id`` / ``base.session_id`` are did_uid: 16 hex, '_', 16 hex. Python
  emitted UUID4 here until DID-python#27, which made every Python document
  schema-invalid under MATLAB while the symmetry suite stayed green -- the
  instance that motivated #28.
* ``base.datestamp`` is ISO-8601 with a 'T' separator and a 'Z' suffix, from
  MATLAB's ``char(datetime(..., 'TimeZone', 'UTCLeapSeconds'))``. Python wrote
  a space-separated, tz-less ``str(datetime.utcnow())`` until 2026-08-29;
  MATLAB validates this field with ``java.time.LocalDateTime.parse``, which
  requires the 'T'. That was #28's second instance and is already closed --
  this test pins the format so it stays closed.
"""

import unittest

from did.validate import ValidationError, validate_docs

# MATLAB's did.ido.unique_id output: 16 hex digits, underscore, 16 hex digits.
MATLAB_ID = "a1b2c3d4e5f60718_293a4b5c6d7e8f90"
MATLAB_SESSION_ID = "0f1e2d3c4b5a6978_8796a5b4c3d2e1f0"
OTHER_ID = "1122334455667788_99aabbccddeeff00"

# char(datetime(..., 'TimeZone', 'UTCLeapSeconds')) -- 'T' separator, 'Z' suffix.
MATLAB_DATESTAMP = "2026-08-30T10:11:16.934Z"


def _base(doc_id=MATLAB_ID, datestamp=MATLAB_DATESTAMP):
    return {
        "session_id": MATLAB_SESSION_ID,
        "id": doc_id,
        "name": "",
        "datestamp": datestamp,
    }


def _demo_a(doc_id=MATLAB_ID, datestamp=MATLAB_DATESTAMP):
    return {
        "document_class": {
            "definition": "$DIDDOCUMENT_EX1/demoA.json",
            "validation": "$DIDSCHEMA_EX1/demoA.schema.json",
            "class_name": "demoA",
            "property_list_name": "demoA",
            "class_version": 1,
            "superclasses": [{"definition": "$DIDDOCUMENT_EX1/base.json"}],
        },
        "base": _base(doc_id, datestamp),
        "demoA": {"value": 1},
    }


def _demo_c(doc_id=OTHER_ID, depends_on=None):
    return {
        "document_class": {
            "definition": "$DIDDOCUMENT_EX1/demoC.json",
            "validation": "$DIDSCHEMA_EX1/demoC.schema.json",
            "class_name": "demoC",
            "property_list_name": "demoC",
            "class_version": 1,
            "superclasses": [{"definition": "$DIDDOCUMENT_EX1/base.json"}],
        },
        "base": _base(doc_id),
        "depends_on": (
            depends_on
            if depends_on is not None
            else [
                {"name": "item1", "value": ""},
                {"name": "item2", "value": ""},
                {"name": "item3", "value": ""},
            ]
        ),
        "demoC": {"value": 1},
    }


def _validate(*docs):
    all_ids = sorted({str(d["base"]["id"]).lower() for d in docs})
    validate_docs(list(docs), all_ids)


class TestMatlabShapedDocumentValidates(unittest.TestCase):
    def test_a_matlab_shaped_demo_a_validates(self):
        _validate(_demo_a())

    def test_a_matlab_shaped_demo_c_with_dependencies_validates(self):
        a = _demo_a()
        c = _demo_c(depends_on=[{"name": "item1", "value": MATLAB_ID}])
        _validate(a, c)

    def test_the_matlab_datestamp_format_is_accepted(self):
        """'T' separator plus 'Z'. MATLAB requires the 'T'; both now emit it."""
        _validate(_demo_a(datestamp=MATLAB_DATESTAMP))

    def test_a_did_uid_id_is_accepted_and_a_uuid4_is_not(self):
        """The #28 instance: base.id is did_uid, and UUID4 is not one."""
        _validate(_demo_a(doc_id=MATLAB_ID))

        with self.assertRaises(ValidationError) as caught:
            _validate(_demo_a(doc_id="3f2504e0-4f89-11d3-9a0c-0305e82c3301"))
        self.assertEqual(caught.exception.identifier, "DID:Database:ValidationFieldUID")

    def test_python_writes_what_it_would_accept_from_matlab(self):
        """A Python-built document must satisfy the same rules.

        Both halves of the contract in one place: if Python ever drifts back
        to a format MATLAB rejects, the fields checked above are where it
        would show.
        """
        from did.document import Document

        doc = Document("demoA", **{"demoA.value": 1})
        props = doc.document_properties

        datestamp = props["base"]["datestamp"]
        self.assertIn("T", datestamp, "MATLAB parses this with LocalDateTime")
        self.assertTrue(datestamp.endswith("Z"))

        _validate(props)


if __name__ == "__main__":
    unittest.main()
