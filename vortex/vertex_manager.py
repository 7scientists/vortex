

class VertexManager(object):

    def __init__(self,edge_type,use_cache = True):
        self._edge_type = edge_type
        self._use_cache = use_cache

    def __get__(self,instance,owner):
        if instance == None:
            raise AttributeError("Vertex descriptor must be called on class instance!")
        if not hasattr(instance,'_vertex_proxy_'+self._edge_type):
            vertex_proxy = instance.VertexProxy(instance,edge_type = self._edge_type,use_cache = self._use_cache)
            setattr(instance,'_vertex_proxy_'+self._edge_type,vertex_proxy)
        return getattr(instance,'_vertex_proxy_'+self._edge_type)

    def __set__(self,instance,value):
        raise AttributeError("Cannot set descriptor!")
