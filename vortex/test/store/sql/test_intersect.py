
from sqlalchemy.types import String,VARCHAR
from vortex.sql import Store

from . import StoreTest


class BasicSqlStoreTest(StoreTest):

    class Meta(Store.Meta):
        vertex_indexes = {'node_type' : {'type' : VARCHAR(64)},'foo' : {'type' : VARCHAR(128)}}

    def test_intersect(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})
        vertex_b = self.store.create_vertex({'foo' : 'baz','node_type' : 'functiondef'})

        s1 = self.store.filter({'node_type' : 'functiondef'})
        s2 = self.store.filter({'foo' : 'bar'})

        assert len(s1) == 2
        assert len(s2) == 1
        assert len(s2.intersect(s1)) == 1