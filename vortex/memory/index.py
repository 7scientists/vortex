from collections import defaultdict

class Index(object):

    def __init__(self,field = None):
        self._index = defaultdict(dict)
        self._reverse_index = defaultdict(dict)
        self._field = field

    def add_vertex(self,vertex,value = None):
        if not self._field in vertex and not value:
            return

        if value is None:
            value = vertex[self._field]

        self._index[value][vertex['pk']] = vertex
        self._reverse_index[vertex['pk']][value] = vertex

    def all_vertices(self):
        return [vertex for values in self._index.values() for vertex in values.values()]

    def vertices_for_value(self,value):
        if callable(value):
            matched_values = [v for v in self._index if value(v)]
            return [vertex for v in matched_values for vertex in self._index[v].values()]
        if value in self._index:
            return self._index[value].values()
        return []
