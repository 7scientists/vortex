from . import StoreTest
from vortex.vertex import Vertex

class BasicSqlStoreTest(StoreTest):

    def test_basic_graph(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})
        vertex_a_rl = self.store.get_vertex(vertex_a.pk) 

        print(vertex_a.data)

        assert vertex_a_rl == vertex_a
        assert vertex_a_rl['foo'] == vertex_a['foo']

        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'global'})
        vertex_c = self.store.create_vertex({'foo' : 'bazzr','node_type' : 'functiondef'})
        edge_ab = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "body",data = {'i' : 0})

        edge_ab_rl = self.store.get_edge({'pk' : edge_ab.pk,'label' : 'body'})
        functiondefs = self.store.filter({'node_type' : 'functiondef'})

        assert len(functiondefs) == 2

        assert len(self.store.filter_edges({'pk' : edge_ab.pk,'label' : 'foo'})) == 0
        assert len(self.store.filter_edges({'label' : 'body'})) == 1

        functiondefs = list(functiondefs)

        with self.assertRaises(Vertex.DoesNotExist):
            self.store.get({'pk' : 'sfdsdfs'})

        with self.assertRaises(Vertex.DoesNotExist):
            self.store.get_vertex('dsfdsfsf')

        assert vertex_a in functiondefs
        assert vertex_c in functiondefs

        assert edge_ab_rl == edge_ab
        assert edge_ab_rl.data == edge_ab.data
        assert edge_ab_rl.label == edge_ab.label
        assert edge_ab_rl.inV == edge_ab.inV
        assert edge_ab_rl.outV == edge_ab.outV
        assert vertex_a.outE[0] == edge_ab
        assert vertex_b.inE[0] == edge_ab

        assert edge_ab in vertex_a.outE
        assert edge_ab in vertex_b.inE