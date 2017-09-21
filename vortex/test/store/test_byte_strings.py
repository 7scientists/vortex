# -*- coding: utf-8 -*-

from . import StoreTest

class ByteStringTest(StoreTest):

    def test_byte_strings(self):

        vertex_a = self.store.create_vertex({'foo' : 'bar','node_type' : 'functiondef'})

        vertex_a_rl = self.store.get_vertex(vertex_a.pk) 

        assert vertex_a_rl == vertex_a
        assert vertex_a_rl['foo'] == vertex_a['foo']

        #We try to create a vertex with a byte-string with non-standard characters (>128)
        vertex_b = self.store.create_vertex({'foo' : 'baz',u'noööääde_"§$$%"!§!§!§"!§"!§$type' : {'global' : u'!§"!"§!"§"$!§!°"$§"§$§"%$§&$%&&%/&(/()/()$§$DSFSDWAERAfdsadlsfd saflasüpfdl saüdflawüpl3432üpl4ü324l'}})

