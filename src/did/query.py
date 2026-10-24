from typing import ClassVar


class Query:
    VALID_OPS: ClassVar[set[str]] = {
        "regexp",
        "exact_string",
        "exact_string_anycase",
        "contains_string",
        "exact_number",
        "lessthan",
        "lessthaneq",
        "greaterthan",
        "greaterthaneq",
        "hassize",
        "hasmember",
        "hasfield",
        "partial_struct",
        "hasanysubfield_contains_string",
        "hasanysubfield_exact_string",
        "or",
        "depends_on",
        "isa",
    }

    def __init__(self, field=None, op=None, param1=None, param2=None):
        if op:
            check_op = op.removeprefix("~")
            if check_op.lower() not in self.VALID_OPS:
                raise ValueError(f"Invalid operator: {op}")

        if isinstance(field, dict):
            self.search_structure = field
        elif isinstance(field, list):
            self.search_structure = self.search_cell_array_to_search_structure(field)
        elif field is not None:
            self.search_structure = self._create_search_structure(
                field, op, param1, param2
            )
        else:
            self.search_structure = []

    def _create_search_structure(self, field, op, param1, param2):
        return [{"field": field, "operation": op, "param1": param1, "param2": param2}]

    @staticmethod
    def search_cell_array_to_search_structure(search_cell_array):
        # Simplified version of the Matlab static method
        search_structure = []
        for i in range(0, len(search_cell_array), 2):
            field = search_cell_array[i]
            value = search_cell_array[i + 1]
            # MATLAB branches on ischar: a char gets 'regexp', everything
            # else gets 'exact_number'. Branching on int/float instead sent a
            # cell/list or struct/dict to 'regexp', where MATLAB sends it to
            # 'exact_number'. Strings and numbers agreed either way; logicals
            # agreed only because bool subclasses int, which this no longer
            # relies on.
            op = "regexp" if isinstance(value, str) else "exact_number"
            search_structure.append(
                {"field": field, "operation": op, "param1": value, "param2": None}
            )
        return search_structure

    def __and__(self, other):
        if not isinstance(other, Query):
            return NotImplemented

        new_query = Query()
        new_query.search_structure = self.search_structure + other.search_structure
        return new_query

    def __or__(self, other):
        if not isinstance(other, Query):
            return NotImplemented

        return Query(
            field="",
            op="or",
            param1=self.search_structure,
            param2=other.search_structure,
        )

    def to_search_structure(self):
        """Resolve high-level operations (isa, depends_on) into lower-level ones.

        This matches MATLAB's query.to_searchstructure which converts:
        - 'isa' -> OR(hasanysubfield_contains_string on superclasses, exact_string on class)
        - 'depends_on' -> hasanysubfield_exact_string on depends_on
        """
        return self._resolve_search_structure(self.search_structure)

    @staticmethod
    def _resolve_search_structure(ss):
        """Resolve a search structure: a list (an AND) or a single item.

        A chain of ORs built in a loop (``q = q | ...``) nests one level per
        term, so this walks the structure with an explicit stack rather than
        by recursion, which ran out of stack after about 1,000 terms (issue
        #100). Each node is rebuilt once all its children are: a list from
        its resolved items, an OR from its resolved operands, anything else
        by :meth:`_resolve_leaf`. The result is what the recursive
        definition gives -- see :meth:`_resolve_single` for the per-node rule.
        """
        # A node is (object, kind). "struct" is anything an OR operand or the
        # top level may be (a list is an AND of items); "single" is one item.
        done = {}  # (id(object), kind) -> resolved
        keep_alive = []  # objects whose id() is a key in done
        stack = [(ss, "struct")]
        while stack:
            node, kind = stack[-1]
            key = (id(node), kind)
            if key in done:
                stack.pop()
                continue
            children = Query._resolve_children(node, kind)
            pending = [c for c in children if (id(c[0]), c[1]) not in done]
            if pending:
                stack.extend(reversed(pending))
                continue
            stack.pop()
            keep_alive.append(node)
            done[key] = Query._resolve_node(node, kind, done)
        return done[(id(ss), "struct")]

    @staticmethod
    def _is_or(item):
        return (
            isinstance(item, dict)
            and str(item.get("operation", "")).removeprefix("~").lower() == "or"
        )

    @staticmethod
    def _resolve_children(node, kind):
        """The nodes that must be resolved before *node* can be."""
        if kind == "struct":
            if isinstance(node, list):
                return [(item, "single") for item in node]
            return [(node, "single")]
        if Query._is_or(node):
            return [
                (node.get(p), "struct") for p in ("param1", "param2") if node.get(p)
            ]
        return []

    @staticmethod
    def _resolve_node(node, kind, done):
        """Resolve *node*, whose children are already resolved in *done*."""
        if kind == "struct":
            if isinstance(node, list):
                return [done[(id(item), "single")] for item in node]
            return done[(id(node), "single")]
        if Query._is_or(node):
            p1, p2 = node.get("param1"), node.get("param2")
            return {
                "field": node.get("field", ""),
                "operation": node.get("operation", ""),
                "param1": done[(id(p1), "struct")] if p1 else p1,
                "param2": done[(id(p2), "struct")] if p2 else p2,
            }
        return Query._resolve_leaf(node)

    @staticmethod
    def _resolve_single(item):
        """Resolve one item: an OR's operands in turn, or a leaf operation."""
        if Query._is_or(item):
            return Query._resolve_search_structure(item)
        return Query._resolve_leaf(item)

    @staticmethod
    def _resolve_leaf(item):
        if not isinstance(item, dict):
            return item

        operation = item.get("operation", "")
        negation = False
        op = operation
        if op.startswith("~"):
            negation = True
            op = op[1:]
        op_lower = op.lower()

        if op_lower == "isa":
            # We keep 'isa' unresolved for field_search (which handles it directly)
            # and let the SQL search handle it via its own isa logic.
            # This avoids breaking the brute-force field_search path which
            # works with both DID-python and MATLAB document formats.
            return item

        elif op_lower == "depends_on":
            name_param = item.get("param1", "")
            value_param = item.get("param2", "")
            param1_list = ["name", "value"]
            param2_list = [name_param, value_param]
            # Wildcard: if name is '*', only match on value
            if name_param == "*":
                param1_list = ["value"]
                param2_list = [value_param]
            resolved = {
                "field": "depends_on",
                "operation": (
                    "~hasanysubfield_exact_string"
                    if negation
                    else "hasanysubfield_exact_string"
                ),
                "param1": param1_list,
                "param2": param2_list,
            }
            return resolved

        return item
