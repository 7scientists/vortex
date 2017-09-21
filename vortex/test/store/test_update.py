from . import StoreTest

class SqlUpdateTest(StoreTest):

    def test_update(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})
        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'global'})
        edge_ab = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "body",data = {'i' : 0})

        assert len(self.store.filter_edges({'label' : 'body'})) == 1

        assert vertex_a.outE[0] == edge_ab
        assert vertex_b.inE[0] == edge_ab

        vertex_a['foo'] = 'baz'

        self.store.update_vertex(vertex_a)

        assert self.store.get({'pk' : vertex_a['pk']})['foo'] == 'baz'

        edge_ab.label = 'booody'

        self.store.update_edge(edge_ab)

        assert len(self.store.filter_edges({'label' : 'body'})) == 0
        assert len(self.store.filter_edges({'label' : 'booody'})) == 1

        assert self.store.get_edge({'label' : 'booody'}).outV == vertex_a
        assert self.store.get_edge({'label' : 'booody'}).inV == vertex_b

        edge_ab.inV,edge_ab.outV = edge_ab.outV,edge_ab.inV

        self.store.update_edge(edge_ab)

        assert self.store.get_edge({'label' : 'booody'}).outV == vertex_b
        assert self.store.get_edge({'label' : 'booody'}).inV == vertex_a
