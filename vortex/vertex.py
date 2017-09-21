from .edge_manager import EdgeManager
from .edge_proxy import EdgeProxy
from .vertex_proxy import VertexProxy
from .vertex_manager import VertexManager
from vortex.utils.serializable import SerializableMixin
from vortex.utils.container import ContainerMixin

from abc import abstractmethod

class Vertex(SerializableMixin,ContainerMixin):

    EdgeProxy = EdgeProxy
    VertexProxy = VertexProxy

    inE = EdgeManager("incoming")
    outE = EdgeManager("outgoing")

    inV = VertexManager("incoming")
    outV = VertexManager("outgoing")

    InE = EdgeManager("incoming",use_cache = False)
    OutE = EdgeManager("outgoing",use_cache = False)

    InV = VertexManager("incoming",use_cache = False)
    OutV = VertexManager("outgoing",use_cache = False)

    class DoesNotExist(BaseException):
        pass

    class MultipleVerticesReturned(BaseException):
        pass

    @property
    def pk(self):
        return self['pk']

    @pk.setter
    def pk(self,pk):
        self['pk'] = pk

    @property
    def attributes(self):
        return self.data_ro
