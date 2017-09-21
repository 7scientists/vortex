
import os

from sqlalchemy import create_engine
from sqlalchemy.types import Integer,String,VARCHAR,Text,Boolean
from vortex.sql import Store

from . import StoreTest

class IndexesTest(StoreTest):

    class Meta(Store.Meta):
        vertex_indexes = {'node_type' : {'type' : VARCHAR(64)},}
        edge_indexes = {'nofollow' : {'type' : Boolean},}

    def test_basic_indexes(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})

        vertex_a_rl = self.store.get_vertex(vertex_a.pk) 

        assert vertex_a_rl == vertex_a
        assert vertex_a_rl['foo'] == vertex_a['foo']

        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'global'})

        vertex_c = self.store.create_vertex({'foo' : 'bazzr','node_type' : 'functiondef'})

        edge_ab = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "body",
                                    data = {'i' : 0,'nofollow' : True})

        edge_ab2 = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "module")

        edge_ab_rl = self.store.get_edge({'pk' : edge_ab.pk})

        with self.assertRaises(AttributeError):
            functiondefs = self.store.filter({'node_type' : 'functiondef','foo' : 'bar'})

        with self.assertRaises(AttributeError):
            self.store.filter_edges({'i' : 0})

        assert len(self.store.filter_edges({'nofollow' : True})) == 1

        self.store.create_vertex_index('foo', {'type' : VARCHAR(128)})
        functiondefs = self.store.filter({'node_type' : 'functiondef','foo' : 'bar'})

        assert len(functiondefs) == 1

class NoIndexesTest(StoreTest):

    class Meta(Store.Meta):
        vertex_indexes = {}
        edge_indexes = {}

    def test_add_index(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})

        vertex_a_rl = self.store.get_vertex(vertex_a.pk) 

        assert vertex_a_rl == vertex_a
        assert vertex_a_rl['foo'] == vertex_a['foo']

        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'global'})

        with self.assertRaises(AttributeError):
            functiondefs = self.store.filter({'node_type' : 'functiondef'})

        self.store.create_vertex_index('node_type',{'type' : VARCHAR(128)})

        assert len(self.store.filter({'node_type' : 'functiondef'})) == 1

        edge_ab = self.store.create_edge(outV = vertex_a,inV = vertex_b,label = "body",data = {'i' : 0,'nofollow' : True})

        with self.assertRaises(AttributeError):
            self.store.filter_edges({'nofollow' : True})

        self.store.create_edge_index('nofollow',{'type' : Boolean})

        assert len(self.store.filter_edges({'nofollow' : True})) == 1
