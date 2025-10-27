from .datastructures.utils import field_search

class Query:
    def __init__(self, field, op=None, param1=None, param2=None):
        if op is None and param1 is None and param2 is None:
            if isinstance(field, dict):
                self.search_structure = field
            elif isinstance(field, list):
                self.search_structure = self._search_cell_array_to_search_structure(field)
            elif isinstance(field, Query):
                self.search_structure = field.search_structure
            else:
                self.search_structure = {}
        else:
            self.search_structure = {
                'field': field,
                'operation': op,
                'param1': param1,
                'param2': param2
            }

    def __and__(self, other):
        new_query = Query({})
        new_query.search_structure = [self.search_structure, other.search_structure]
        return new_query

    def __or__(self, other):
        return Query({
            'field': '',
            'operation': 'or',
            'param1': self.search_structure,
            'param2': other.search_structure
        })

    def to_search_structure(self):
        return self.search_structure

    @staticmethod
    def _search_cell_array_to_search_structure(search_cell_array):
        search_struct = []
        for i in range(0, len(search_cell_array), 2):
            op = 'regexp' if isinstance(search_cell_array[i+1], str) else 'exact_number'
            search_struct.append({
                'field': search_cell_array[i],
                'operation': op,
                'param1': search_cell_array[i+1],
                'param2': None
            })
        return search_struct
