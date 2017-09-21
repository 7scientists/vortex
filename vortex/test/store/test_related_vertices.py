from . import StoreTest
from vortex.vertex import Vertex

import pytest

class OutgoingVerticesTest(StoreTest):

    def test_basic_graph(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})
        vertex_a_rl = self.store.get_vertex(vertex_a.pk) 

        assert vertex_a_rl == vertex_a
        assert vertex_a_rl['foo'] == vertex_a['foo']

        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'global'})
        vertex_c = self.store.create_vertex({'foo' : 'bazzr','node_type' : 'functiondef'})
        edge_ab = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "body",data = {'i' : 0})
        edge_ab_rl = self.store.get_edge({'pk' : edge_ab.pk,'label' : 'body'})
        functiondefs = self.store.filter({'node_type' : 'functiondef'})

        assert vertex_a.outV[0] == vertex_b
        assert vertex_b.inV[0] == vertex_a

        assert vertex_b in vertex_a.outV
        assert vertex_a in vertex_b.inV

        assert vertex_b in vertex_a.outV('body')
        assert vertex_a in vertex_b.inV('body')

        with pytest.raises(TypeError):
            assert 'body' in vertex_a.outV

