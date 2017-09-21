
from collections import defaultdict
from abc import abstractmethod
from .edge import Edge

class VertexProxy(object):

    def __init__(self,instance,edge_type,label = None,use_cache = True):
        self._instance = instance
        self._edge_type = edge_type
        self._use_cache = use_cache

    def get_edge_list(self):
        if self._edge_type == 'incoming':
            if self._use_cache:
                inE = self._instance.inE
            else:
                inE = self._instance.InE
            return inE.get_edge_list()
        else:
            if self._use_cache:
                outE = self._instance.outE
            else:
                outE = self._instance.OutE
            return outE.get_edge_list()

    def get_edges_by_label(self):
        edges = self.get_edge_list()
        edges_by_label = defaultdict(list)
        for edge in edges:
            edges_by_label[edge.label].append(edge)
        return edges_by_label

    def __call__(self,label = None,data = None):
        if label is None:
            edge_list = self.get_edge_list()
            if self._edge_type == 'incoming':
                return [edge.outV for edge in edge_list]
            else:
                return [edge.inV for edge in edge_list]
        else:
            if self._edge_type == 'incoming':
                if data is None:
                    return [edge.outV for edge in self.get_edges_by_label()[label]]
                else:
                    candidate_edges = self.get_edges_by_label()[label]
                    return [edge.outV for edge in candidate_edges \
                            if all([True if key in edge.data \
                                    and edge.data[key] == data[key] \
                                    else False for key in data])]
            else:
                if data is None:
                    return [edge.inV for edge in self.get_edges_by_label()[label]]
                candidate_edges = self.get_edges_by_label()[label]
                return [edge.inV for edge in candidate_edges
                        if all([True if edge.data and key in edge.data
                                and edge.data[key] == data[key]
                                else False for key in data])]

    def __len__(self):
        return len(self.get_edge_list())

    def __contains__(self,vertex):
        from .vertex import Vertex

        if not isinstance(vertex,Vertex):
            raise TypeError("Not a vertex!")

        if self._edge_type == 'incoming':
            return True if vertex in [edge.outV for edge in self.get_edge_list()] else False
        else:
            return True if vertex in [edge.inV for edge in self.get_edge_list()] else False

    def keys(self):
        return self.get_edges_by_label().keys()

    def values(self):
        if self._edge_type == 'incoming':
            return [edge.outV for edge in self.get_edges_by_label().values()]
        else:
            return [edge.inV for edge in self.get_edges_by_label().values()]

    def items(self):
        return [(label,[edge.outV if self._edge_type == 'incoming' else edge.inV for edge in edges]) for label,edges in self.get_edges_by_label().items()]

    def __iter__(self):
        for i in range(0,len(self)):
            yield self[i]

    def __getitem__(self,i):
        if self._edge_type == 'incoming':
            return self.get_edge_list()[i].outV
        else:
            return self.get_edge_list()[i].inV
