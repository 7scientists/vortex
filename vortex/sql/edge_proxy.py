from ..edge_proxy import EdgeProxy as BaseEdgeProxy

from collections import defaultdict

class EdgeProxy(BaseEdgeProxy):

    def __init__(self,*args,**kwargs):
        super(EdgeProxy,self).__init__(*args,**kwargs)
        self._incoming_edges = None
        self._outgoing_edges = None

    def set_edge_list(self,edge_list):
        if self._edge_type == 'incoming':
            self._incoming_edges = edge_list
        else:
            self._outgoing_edges = edge_list

    def get_edge_list(self):
        if self._edge_type == "incoming":
            if self._incoming_edges is None or not self._use_cache:
                self._incoming_edges = self._instance.get_incoming_edges()
            return self._incoming_edges
        else:
            if self._outgoing_edges is None or not self._use_cache:
                self._outgoing_edges = self._instance.get_outgoing_edges()
            return self._outgoing_edges