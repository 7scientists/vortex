from . import StoreTest

class SqlInsertionTest(StoreTest):

    def test_insertion(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})
        vertex_a_rl = self.store.get_vertex(vertex_a.pk) 

        assert vertex_a_rl == vertex_a
        assert vertex_a_rl['foo'] == vertex_a['foo']

        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'global'})
        vertex_b = self.store.get_or_create_vertex({'foo' : 'baz','node_type' : 'global'})
        vertex_c = self.store.create_vertex({'foo' : 'bazzr','node_type' : 'functiondef'})
        edge_ab = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "body",data = {'i' : 0})
        edge_ab = self.store.get_or_create_edge(outV = vertex_a,inV = vertex_b,label = "body",data = {'i' : 0})

        assert len(self.store.filter_edges({'pk' : edge_ab.pk,'label' : 'foo'})) == 0
        assert len(self.store.filter_edges({'label' : 'body'})) == 1

        assert vertex_a.outE[0] == edge_ab
        assert vertex_b.inE[0] == edge_ab

        assert edge_ab in vertex_a.outE
        assert edge_ab in vertex_b.inE