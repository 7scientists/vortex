
from collections import defaultdict
from abc import abstractmethod
from .edge import Edge
import six

class EdgeProxy(object):

    def __init__(self,instance,edge_type,label = None,use_cache = True):
        self._instance = instance
        self._use_cache = use_cache
        self._edge_type = edge_type

    @abstractmethod
    def get_edge_list(self):
        raise NotImplementedError

    def get_edges_by_label(self):
        edges = self.get_edge_list()
        edges_by_label = defaultdict(list)
        for edge in edges:
            edges_by_label[edge.label].append(edge)
        return edges_by_label

    def __call__(self,label_or_vertex = None):
        edge_list = self.get_edge_list()
        if label_or_vertex == None:
            return edge_list
        elif isinstance(label_or_vertex,six.string_types):
            return self.get_edges_by_label()[label_or_vertex]
        else:
            return [e for e in edge_list
                    if self._edge_type == "incoming" and e.outV == label_or_vertex
                    or self._edge_type == "outgoing" and e.inV == label_or_vertex
                   ]

    def __len__(self):
        return len(self.get_edge_list())

    def __contains__(self,label_or_edge):
        if isinstance(label_or_edge,Edge):
            return True if label_or_edge in self.get_edge_list() else False
        return True if label_or_edge in self.get_edges_by_label() else False

    def keys(self):
        return self.get_edges_by_label().keys()

    def values(self):
        return self.get_edges_by_label().values()

    def items(self):
        return self.get_edges_by_label().items()

    def __iter__(self):
        for i in range(0,len(self)):
            yield self[i]

    def __getitem__(self,i):
        return self.get_edge_list()[i]