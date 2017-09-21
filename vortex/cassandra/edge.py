from ..edge import Edge as BaseEdge

from functools import wraps
from collections import defaultdict

class Edge(BaseEdge):

    def with_data(f):

        @wraps(f)
        def with_data_wrapper(self,*args,**kwargs):
            if self._db_data is None:
                self._get_db_data()
            return f(self,*args,**kwargs)

        return with_data_wrapper

    def __init__(self,pk,store,db_data = None):
        self._store = store
        self._pk = pk
        self._db_data = db_data
        self._outV = None
        self._inV = None

    def _get_db_data(self):
        self._db_data = self._store.get_edge_data(pk = self._pk)

    def __eq__(self,other):
        return True if isinstance(other,Edge) and other.pk == self.pk else False

    @property
    def pk(self):
        return self._pk

    @property
    @with_data
    def inV(self):
        if self._inV is None:
            self._inV = self._store.get_vertex(self._db_data['inc_v_pk'])
        return self._inV

    @property
    @with_data
    def outV(self):
        if self._outV is None:
            self._outV = self._store.get_vertex(self._db_data['out_v_pk'])
        return self._outV

    @property
    @with_data
    def label(self):
        return self._db_data['label']

    @property
    def data(self):
        return self._db_data['data']
