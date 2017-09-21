from ..vertex import Vertex as BaseVertex

from functools import wraps

from .edge_proxy import EdgeProxy

class Vertex(BaseVertex):

    EdgeProxy = EdgeProxy

    def __init__(self,pk,store,db_data = None):
        super(Vertex,self).__init__(db_data)
        self._store = store
        self._edges = None
        self._outgoing_edges = None
        self._incoming_edges = None
        self['pk'] = pk

    def __eq__(self,other):
        return True if isinstance(other,Vertex) and other.pk == self.pk else False

    @property
    def data_ro(self):
        return self.data_rw

    @property
    def data_rw(self):
        if self._data is None:
            self._get_db_data()
        return self._data

    def _get_db_data(self):
        self._data.update(self._store.get_vertex_data(self.pk))

    def get_outgoing_edges(self):
        return list(self._store.filter_edges({'out_v_pk' : self.pk}))

    def get_incoming_edges(self):
        return list(self._store.filter_edges({'inc_v_pk' : self.pk}))