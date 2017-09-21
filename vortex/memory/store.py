# -*- coding: utf-8 -*-
from ..base import Store as BaseStore

from .edge import Edge
from .vertex import Vertex
from .query import compile_query
from .index import Index
from .queryset import QuerySet

class Store(BaseStore):

    """
    """

    def __init__(self,vertex_index_fields = ['node_type','pk'],edge_index_fields = [],**kwargs):
        super(Store,self).__init__(**kwargs)
        self._vertex_index_fields = vertex_index_fields
        self._edge_index_fields = edge_index_fields
        self.reset()

    def get(self,query):
        results = self.filter(query)
        if len(results) == 0:
            raise Vertex.DoesNotExist
        elif len(results) > 1:
            raise Vertex.MultipleVerticesReturned
        return results.pop()

    def filter(self,query):

        def query_function(key, expression):
            if key == None:
                return set(indexes['pk'].all_vertices())
            qs = indexes[key].vertices_for_value(expression)
            return set(qs)

        indexes_to_create = []
        indexes = self._vertex_indexes
        def index_collector(key, expressions):
            if key not in indexes and key not in indexes_to_create and key != None:
                indexes_to_create.append(key)
            return set([])

        compiled_query = compile_query(query)

        # We collect all the indexes that we need to create
        compiled_query(index_collector)
    
        if indexes_to_create:
            for index in indexes_to_create:
                self.create_index(index)

        return compiled_query(query_function)

    def create_index(self,field):
        if field in self._vertex_indexes:
            return
        index = Index(field)
        self._vertex_indexes[field] = index
        for vertex in self._vertices.values():
            index.add_vertex(vertex)

    @property
    def edges(self):
        return self._edges.values()

    @property
    def vertices(self):
        return self._vertices.values()

    def reset(self):
        self._vertex_indexes = {}
        self._edge_indexes = {}
        self._vertices = {}
        self._edges = {}
        for index_field in self._vertex_index_fields:
            self._vertex_indexes[index_field] = Index(index_field)

    def get_vertex(self,pk):
        try:
            return self._vertices[pk]
        except KeyError:
            raise Vertex.DoesNotExist

    def get_edge(self,pk):
        return self._edges[pk]

    def create_edge(self,vertex_a,vertex_b,label,data = None,check_for_duplicates = True,edge_class = Edge):
        if not vertex_a.pk in self._vertices or vertex_a != self._vertices[vertex_a.pk]:
            raise AttributeError("Cannot add edge to vertex that is not in graph (pk = %s)!" % str(vertex_a.pk) )
        if not vertex_b.pk in self._vertices or vertex_b != self._vertices[vertex_b.pk]:
            raise AttributeError("Cannot add edge to vertex that is not in graph (pk = %s)!" % str(vertex_b.pk) )
        if check_for_duplicates:
            existing_edges = vertex_a.outE(vertex_b)
            for edge in existing_edges:
                if edge.label == label and edge.data == data:
                    return edge
        edge = edge_class(vertex_a,vertex_b,label,data)
        self._edges[edge.pk] = edge
        return edge

    def get_or_create_edge(self,vertex_a,vertex_b,label,data = None,edge_class = Edge):
        return self.create_edge(vertex_a,vertex_b,label,data,edge_class = edge_class)

    def get_or_create_vertex(self,attributes):
        if attributes['pk'] in self._vertices:
            return self._vertices[attributes['pk']]
        return self.create_vertex(attributes)

    def update_vertex(self,vertex):
        if not isinstance(vertex,Vertex):
            raise TypeError("update_vertex must be called with a Vertex instance as argument!")
        if not self.get_vertex(vertex.pk) == vertex:
            raise Vertex.DoesNotExist
        for index in self._vertex_indexes.values():
            index.add_vertex(vertex)
        return vertex

    def create_vertex(self,attributes):
        vertex = Vertex(attributes)
        self._vertices[attributes['pk']] = vertex
        for index in self._vertex_indexes.values():
            index.add_vertex(vertex)
        return vertex
