"""Reading definition files that use MATLAB's bare ``Inf`` token.

MATLAB's ``jsondecode`` accepts ``Inf``, ``-Inf`` and ``NaN`` where JSON has no
literal for them, and shared definition files use them: NDI-matlab writes
``"parameters": [-Inf,Inf,0]`` for a double field with no bounds, and
NDIcalc-vis-matlab writes ``[NaN,NaN]``.

Python's json accepts ``Infinity`` / ``-Infinity`` / ``NaN`` but *not* ``Inf``,
so every such file raised ``DID:Database:ValidationFileBad`` here while MATLAB
read it and validated against it. NDI-matlab's own CI is green on a run that
validates a simple_calc document against
``apps/calculations/simple_calc_v2_schema.json``, which contains ``[-Inf,Inf,0]``
-- so the file is good and the reader was wrong.
"""

import json
import os
import tempfile
import unittest

from did.validate import (
    ValidationError,
    get_document_schema,
    loads_matlab_json,
    validate_field_type_and_value,
)

UNBOUNDED_DOUBLE = {
    "name": "answer",
    "type": "double",
    "default_value": 1,
    "parameters": [float("-inf"), float("inf"), 0],
}


class TestLoadsMatlabJson(unittest.TestCase):
    def test_bare_inf(self):
        self.assertEqual(
            loads_matlab_json("[-Inf,Inf,0]"), [float("-inf"), float("inf"), 0]
        )

    def test_bare_nan(self):
        import math

        values = loads_matlab_json("[NaN,NaN]")
        self.assertEqual(len(values), 2)
        self.assertTrue(all(math.isnan(v) for v in values))

    def test_plain_json_is_unchanged(self):
        text = '{"a": [0, 1.5, -2], "b": "x", "c": null, "d": true}'
        self.assertEqual(loads_matlab_json(text), json.loads(text))

    def test_inf_inside_a_string_is_left_alone(self):
        """Documentation text mentioning Inf must not be rewritten."""
        text = '{"doc": "ranges from -Inf to Inf", "p": [-Inf,Inf,0]}'
        parsed = loads_matlab_json(text)
        self.assertEqual(parsed["doc"], "ranges from -Inf to Inf")
        self.assertEqual(parsed["p"], [float("-inf"), float("inf"), 0])

    def test_infinity_and_info_are_not_mangled(self):
        parsed = loads_matlab_json('{"a": Infinity, "b": -Infinity, "c": "Info"}')
        self.assertEqual(parsed["a"], float("inf"))
        self.assertEqual(parsed["b"], float("-inf"))
        self.assertEqual(parsed["c"], "Info")

    def test_genuinely_broken_json_still_raises(self):
        """Tolerating Inf must not turn the parser into a guesser."""
        for text in ['{"a": ,}', '{"a": 1,}', "{", '{"a" 1}']:
            with self.assertRaises(ValueError):
                loads_matlab_json(text)


class TestSchemaWithInfParameters(unittest.TestCase):
    """The end-to-end path: a schema file on disk with MATLAB's Inf in it."""

    def setUp(self):
        self._dir = tempfile.mkdtemp()

    def _schema_file(self, body):
        path = os.path.join(self._dir, "thing_schema.json")
        with open(path, "w") as handle:
            handle.write(body)
        return path

    def test_schema_file_with_inf_parameters_loads(self):
        path = self._schema_file(
            '{"classname": "thing", "superclasses": [],\n'
            ' "thing": [{"name": "answer", "type": "double",\n'
            '            "default_value": 1, "parameters": [-Inf,Inf,0]}]}'
        )
        schema = get_document_schema(path)
        self.assertEqual(
            schema["thing"][0]["parameters"], [float("-inf"), float("inf"), 0]
        )

    def test_a_file_that_is_not_json_still_reports_ValidationFileBad(self):
        path = self._schema_file('{"classname": "thing",}')
        with self.assertRaises(ValidationError) as caught:
            get_document_schema(path)
        self.assertEqual(caught.exception.identifier, "DID:Database:ValidationFileBad")


class TestUnboundedDoubleValidates(unittest.TestCase):
    """Infinite bounds have to behave as bounds, not just parse."""

    def test_any_finite_value_is_accepted(self):
        for value in (0, -1e300, 1e300, 42.5):
            validate_field_type_and_value(
                "doc", "thing.answer", value, UNBOUNDED_DOUBLE
            )

    def test_nan_is_still_governed_by_the_third_parameter(self):
        """parameters[2] == 0 means NaN is not allowed, infinite bounds or not."""
        with self.assertRaises(ValidationError):
            validate_field_type_and_value(
                "doc", "thing.answer", float("nan"), UNBOUNDED_DOUBLE
            )


if __name__ == "__main__":
    unittest.main()
