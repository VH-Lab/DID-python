class Query:
    VALID_OPS = {
        'regexp', 'exact_string', 'exact_string_anycase', 'contains_string', 'exact_number',
        'lessthan', 'lessthaneq', 'greaterthan', 'greaterthaneq', 'hassize', 'hasmember',
        'hasfield', 'partial_struct', 'hasanysubfield_contains_string', 'hasanysubfield_exact_string',
        'or', 'depends_on', 'isa'
    }

    def __init__(self, field=None, op=None, param1=None, param2=None):
        if op:
            check_op = op[1:] if op.startswith('~') else op
            if check_op.lower() not in self.VALID_OPS:
                raise ValueError(f"Invalid operator: {op}")

        if isinstance(field, dict):
            self.search_structure = field
        elif isinstance(field, list):
            self.search_structure = self.search_cell_array_to_search_structure(field)
        elif field is not None:
            self.search_structure = self._create_search_structure(field, op, param1, param2)
        else:
            self.search_structure = []

    def _create_search_structure(self, field, op, param1, param2):
        return [{'field': field, 'operation': op, 'param1': param1, 'param2': param2}]

    @staticmethod
    def search_cell_array_to_search_structure(search_cell_array):
        # Simplified version of the Matlab static method
        search_structure = []
        for i in range(0, len(search_cell_array), 2):
            field = search_cell_array[i]
            value = search_cell_array[i+1]
            op = 'exact_number' if isinstance(value, (int, float)) else 'regexp'
            search_structure.append({'field': field, 'operation': op, 'param1': value, 'param2': None})
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

        return Query(field='', op='or', param1=self.search_structure, param2=other.search_structure)

    def to_search_structure(self):
        """Resolve high-level operations (isa, depends_on) into lower-level ones.

        This matches MATLAB's query.to_searchstructure which converts:
        - 'isa' -> OR(hasanysubfield_contains_string on superclasses, exact_string on class)
        - 'depends_on' -> hasanysubfield_exact_string on depends_on
        """
        return self._resolve_search_structure(self.search_structure)

    @staticmethod
    def _resolve_search_structure(ss):
        """Recursively resolve a search structure."""
        if isinstance(ss, list):
            return [Query._resolve_single(item) for item in ss]
        return Query._resolve_single(ss)

    @staticmethod
    def _resolve_single(item):
        if not isinstance(item, dict):
            return item

        operation = item.get('operation', '')
        negation = False
        op = operation
        if op.startswith('~'):
            negation = True
            op = op[1:]
        op_lower = op.lower()

        if op_lower == 'isa':
            classname = item.get('param1', '')
            # We keep 'isa' unresolved for field_search (which handles it directly)
            # and let the SQL search handle it via its own isa logic.
            # This avoids breaking the brute-force field_search path which
            # works with both DID-python and MATLAB document formats.
            return item

        elif op_lower == 'depends_on':
            name_param = item.get('param1', '')
            value_param = item.get('param2', '')
            param1_list = ['name', 'value']
            param2_list = [name_param, value_param]
            # Wildcard: if name is '*', only match on value
            if name_param == '*':
                param1_list = ['value']
                param2_list = [value_param]
            resolved = {
                'field': 'depends_on',
                'operation': '~hasanysubfield_exact_string' if negation else 'hasanysubfield_exact_string',
                'param1': param1_list,
                'param2': param2_list
            }
            return resolved

        elif op_lower == 'or':
            # Recursively resolve OR sub-structures
            p1 = item.get('param1')
            p2 = item.get('param2')
            return {
                'field': item.get('field', ''),
                'operation': operation,
                'param1': Query._resolve_search_structure(p1) if p1 else p1,
                'param2': Query._resolve_search_structure(p2) if p2 else p2
            }

        return item