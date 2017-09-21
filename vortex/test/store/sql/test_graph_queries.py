
import unittest
import os
import six
import gzip

from vortex.sql import Store,Vertex,Edge

from sqlalchemy import create_engine
from sqlalchemy.types import String

from . import SqlStoreTest

def is_descendant_of(vertex,ancestor,visited = None):
    if not visited:
        visited = {}
    if vertex['pk'] in visited:
        return False
    visited[vertex['pk']] = True
    for inE in vertex.inE:
        if inE.outV == ancestor:
            return True
        if is_descendant_of(inE.outV,ancestor,visited = visited):
            return True
    return False

def is_ancestor_of(vertex,descendant,visited = None):
    if not visited:
        visited = {}
    if vertex['pk'] in visited:
        return False
    visited[vertex['pk']] = True
    for outE in vertex.outE:
        if outE.inV == descendant:
            return True
        if is_ancestor_of(outE.inV,descendant,visited = visited):
            return True
    return False

class DescendantsAndAncestorsTest(SqlStoreTest):

    def setup_vertices(self):

        trans = self.store.begin()

        self.m = self.store.get_or_create_vertex({'node_type' : 'module'})
        self.c = self.store.get_or_create_vertex({'node_type' : 'classdef'})
        self.store.get_or_create_edge(self.m,self.c,'body',{'i' : 0})

        self.f1 = self.store.get_or_create_vertex({'node_type' : 'functiondef'})
        self.store.get_or_create_edge(self.c,self.f1,'body',{'i' : 0})
        self.a1 = self.store.get_or_create_vertex({'node_type' : 'assignment'})
        self.store.get_or_create_edge(self.f1,self.a1,'body',{'i' : 0})
        self.a2 = self.store.get_or_create_vertex({'node_type' : 'assignment'})
        self.store.get_or_create_edge(self.f1,self.a2,'body',{'i' : 1})

        self.store.commit(trans)
       
    def test_relations(self):

        self.setup_vertices()

        assert is_descendant_of(self.f1,self.m)
        assert is_descendant_of(self.a2,self.m)
        assert is_descendant_of(self.a1,self.c)
        assert not is_descendant_of(self.c,self.a1)
        assert not is_descendant_of(self.m,self.a2)
        assert not is_descendant_of(self.m,self.f1)


    def test_cyclic_relation(self):

        self.setup_vertices()

        self.store.get_or_create_edge(self.f1,self.m,'defined_in')
        assert is_descendant_of(self.m,self.f1)


    def test_descendants(self):

        self.setup_vertices()

        for vertex in self.store.filter({'node_type' : 'module'}):

            descendants = self.store.descendants_of(vertex)
            assert len(descendants) == 4

            cnt = 0
            for descendent_vertex in descendants:
                cnt+=1
                assert is_descendant_of(descendent_vertex,vertex)

            assert cnt == len(descendants)

    def test_ancestors(self):

        self.setup_vertices()

        for vertex in self.store.filter({'node_type' : 'assignment'}):

            ancestors = self.store.ancestors_of(vertex,with_self = False)

            print(list(ancestors)[0].extra,vertex['pk'])
            assert len(ancestors) == 3

            cnt = 0
            for ancestor_vertex in ancestors:
                cnt+=1
                assert is_ancestor_of(ancestor_vertex,vertex)

            assert cnt == len(ancestors)


    def test_ancestors_as_tree(self):

        self.setup_vertices()

        for vertex in self.store.filter({'node_type' : 'assignment'}):

            for vertex_or_pk in [vertex,vertex['pk']]:

                v = self.store.ancestors_of(vertex_or_pk,as_tree = True)

                assert hasattr(v.inE,'_incoming_edges') and v.inE._incoming_edges is not None

                assert self.f1 in [e.outV for e in v.inE._incoming_edges]
                assert self.f1 in [e.outV for e in v.inE('body')]

                f1 = filter(lambda v:v == self.f1,[e.outV for e in v.inE._incoming_edges])

                if six.PY3:
                    f1 = next(f1)
                else:
                    f1 = f1[0]

                assert hasattr(f1.inE,'_incoming_edges') and f1.inE._incoming_edges is not None

                assert self.c in [e.outV for e in f1.inE._incoming_edges]


    def test_descendants_as_tree(self):

        self.setup_vertices()

        for vertex_or_pk in [self.c['pk'],self.c]:

            v = self.store.descendants_of(vertex_or_pk,as_tree = True)

            assert hasattr(v.outE,'_outgoing_edges') and v.outE._outgoing_edges is not None

            assert self.f1 in [e.inV for e in v.outE._outgoing_edges]
            assert self.f1 in [e.inV for e in v.outE('body')]

            f1 = filter(lambda v:v == self.f1,[e.inV for e in v.outE._outgoing_edges])

            if six.PY3:
                f1 = next(f1)
            else:
                f1 = f1[0]

            assert hasattr(f1.outE,'_outgoing_edges') and f1.outE._outgoing_edges is not None

            assert self.a1 in [e.inV for e in f1.outE._outgoing_edges]


    def test_ancestors_and_descendants(self):

        self.setup_vertices()

        qs1 = self.store.descendants_of(self.m)
        qs2 = self.store.ancestors_of(self.f1)
        qs3 = self.store.filter({'node_type' : 'classdef'})

        assert len(qs1) == 4
        assert len(qs2) == 2

        qs = list(qs1.intersect(qs2).intersect(qs3))

        assert len(qs) == 1

        assert qs[0] == self.c

        for vertex in qs:

            assert is_ancestor_of(vertex,self.f1)
            assert is_descendant_of(vertex,self.m)
            assert vertex['node_type'] == 'classdef'