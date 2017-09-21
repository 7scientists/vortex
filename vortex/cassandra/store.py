# -*- coding: utf-8 -*-
from ..base import Store as BaseStore

from .edge import Edge
from .vertex import Vertex
from .queryset import QuerySet

import pyorient

import cPickle
import base64
import uuid

class Store(BaseStore):

    """
    """

    class Meta(BaseStore.Meta):

        table_postfix = ""
        vertex_indexes = {}
        edge_indexes = {}
        PkType = str
        Vertex = Vertex
        Edge = Edge

    def begin(self):
        return None

    def commit(self,transaction):
        pass

    def rollback(self,transaction):
        pass

    def close_connection(self):
        pass

    def create_schema(self,indexes = None):
        pass

    def drop_schema(self):
        pass

    def __init__(self,client,table_postfix = None,create_schema = True,use_cache = False,**kwargs):
        super(Store,self).__init__(**kwargs)
        self._client = client
        self._vertex_cache = {}
        self._use_cache = use_cache
        self.table_postfix = table_postfix if table_postfix is not None else self.Meta.table_postfix
        self.vertex_indexes = self.Meta.vertex_indexes.copy()
        self.edge_indexes = self.Meta.edge_indexes.copy()
        if create_schema:
            self.create_schema()

    def get_edge(self,query):
        pass

    def get(self,query):
        pass

    def filter(self,query):
        pass

    def filter_vertices(self,query):
        pass

    def filter_edges(self,query):
        pass

    @property
    def edges(self):
        pass

    @property
    def vertices(self):
        pass

    def reset(self):
        self.drop_schema()
        self.create_schema()


    def get_incoming_edges(self,pk):
        pass

    def get_outgoing_edges(self,pk):
        pass

    def get_vertex(self,pk):
        pass

    def create_edge(self,outV,inV,label,data = None):
        serialized_data = cPickle.dumps(data)
        pk = uuid.uuid4().hex
        d = {'data' : serialized_data,
             'pk' : pk,
             'inc_v_pk' : inV.pk,
             'out_v_pk' : outV.pk,
             'label' : label,
             }
        #...
        return self.Meta.Edge(pk,store = self,db_data = {'label' : label,
                                               'data' : data,
                                               'pk' : pk,
                                               'out_v_pk' : outV.pk,
                                               'inc_v_pk' : inV.pk})

    def get_or_create_edge(self,outV,inV,label,data = None):
        if not isinstance(outV,self.Meta.Vertex):
            outV = self.get_vertex(outV['pk'])
        if not isinstance(inV,self.Meta.Vertex):
            inV = self.get_vertex(inV['pk'])
        outgoing_edges = self.filter_edges({'out_v_pk' : outV['pk'],
                                            'inc_v_pk' : inV['pk'],
                                            'label' : label})
        for edge in outgoing_edges:
            if edge.data == data:
                return edge
        return self.create_edge(outV,inV,label,data)

    def get_or_create_vertex(self,attributes):
        if 'pk' in attributes:
            try:
                return self.get_vertex(attributes['pk'])
            except self.Meta.Vertex.DoesNotExist:
                pass
        return self.create_vertex(attributes)

    def update_vertex(self,vertex):
        if not isinstance(vertex,self.Meta.Vertex):
            raise TypeError("Must be a vertex!")
        return vertex

    def serialize_vertex_data(self,data):
        return data

    def create_vertex(self,data):
        d = self.serialize_vertex_data(data)
        if not 'pk' in data:
            d['pk'] = uuid.uuid4().hex
        else:
            d['pk'] = data['pk']
        return self.Meta.Vertex(d['pk'],store = self,db_data = data)
