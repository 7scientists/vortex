from ..edge_proxy import EdgeProxy as BaseEdgeProxy

from collections import defaultdict

class EdgeProxy(BaseEdgeProxy):

    def get_edge_list(self):
        if self._edge_type == "incoming":
            attr = '_inE'
        else:
            attr = '_outE'
        if not hasattr(self._instance,attr):
            return []
        return [v for l in getattr(self._instance,attr).values() for v in l.values()]

    def append(self,edge):
        if self._edge_type == "incoming":
            if not hasattr(self._instance,'_inE'):
                self._instance._inE = defaultdict(dict)
            ed = self._instance._inE
        else:
            if not hasattr(self._instance,'_outE'):
                self._instance._outE = defaultdict(dict)
            ed = self._instance._outE
        if not edge.pk in ed[edge.label]:
            ed[edge.label][edge.pk] = edge