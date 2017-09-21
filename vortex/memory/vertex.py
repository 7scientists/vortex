from ..vertex import Vertex as BaseVertex
from .edge_proxy import EdgeProxy
from vortex.utils.serializable import SerializableMixin
from vortex.utils.container import ContainerMixin

class Vertex(BaseVertex):

    EdgeProxy = EdgeProxy

    def _get_full_dict(self):
        d = self._data.copy()
        d.update(self.outE.vertices())
        return d

    @property
    def data_ro(self):
        return self._get_full_dict()

    @property
    def data_rw(self):
        return self._data

    @property
    def pk(self):
        return self.data_ro['pk']

    @pk.setter
    def pk(self,value):
        self.data_rw['pk'] = value