class Query:
    def __init__(self, field=None, op=None, param1=None, param2=None):
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
        # A full implementation would recursively resolve 'isa', 'depends_on', etc.
        # This is a simplified version for now.
        return self.search_structure