

class EdgeManager(object):

    def __init__(self,edge_type,use_cache = True):
        self._edge_type = edge_type
        self._use_cache = use_cache

    def __get__(self,instance,owner):
        if instance == None:
            raise AttributeError("Edge descriptor must be called on class instance!")
        if not hasattr(instance,'_edge_proxy_'+self._edge_type):
            edge_proxy = instance.EdgeProxy(instance,
                                            edge_type = self._edge_type,
                                            use_cache = self._use_cache)
            setattr(instance,'_edge_proxy_'+self._edge_type,edge_proxy)
        return getattr(instance,'_edge_proxy_'+self._edge_type)

    def __set__(self,instance,value):
        raise AttributeError("Cannot set descriptor!")
