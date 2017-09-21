from . import StoreTest

class SqlDeleteTest(StoreTest):

    def test_delete_vertex(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})
        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'global'})
        edge_ab = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "body",data = {'i' : 0})

        assert len(self.store.filter_edges({'label' : 'body'})) == 1

        assert vertex_a.outE[0] == edge_ab
        assert vertex_b.inE[0] == edge_ab

        vertex_a['foo'] = 'baz'

        self.store.delete_vertex(vertex_a)


        with self.assertRaises(self.store.Meta.Vertex.DoesNotExist):
            self.store.get({'pk' : vertex_a['pk']})

        with self.assertRaises(self.store.Meta.Edge.DoesNotExist):
            self.store.get_edge({'label' : 'body'})

    def test_delete_edge(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})
        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'global'})
        edge_ab = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "body",data = {'i' : 0})

        assert len(self.store.filter_edges({'label' : 'body'})) == 1

        assert vertex_a.outE[0] == edge_ab
        assert vertex_b.inE[0] == edge_ab

        vertex_a['foo'] = 'baz'

        self.store.delete_edge(edge_ab)

        with self.assertRaises(self.store.Meta.Edge.DoesNotExist):
            self.store.get_edge({'label' : 'body'})
