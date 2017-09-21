# -*- coding: utf-8 -*-
from ..base import Store as BaseStore

from .edge import Edge
from .vertex import Vertex
from .queryset import QuerySet

from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import MetaData,Table,Column,ForeignKey,UniqueConstraint
from sqlalchemy.types import Integer,VARCHAR,String,Text,LargeBinary,Unicode
from sqlalchemy.sql import select,insert,update,func,and_,or_,not_,expression

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
        PkType = VARCHAR(64)
        Vertex = Vertex
        Edge = Edge

    def create_vertex_index(self,key,params,initialize = True,overwrite = False):
        if key in self.vertex_indexes and not overwrite:
            return
        
        self.vertex_indexes[key] = params

        self.init_schema()
        self.create_schema()

        index_tables = {key : self._vertex_index_tables[key]}
        if initialize:
            trans = self.begin()
            for vertex in self.filter({}):
                self._add_to_index(vertex.pk,vertex.data_rw,index_tables)
            self.commit(trans)

    def create_edge_index(self,key,params,initialize = True,overwrite = False):
        if key in self.edge_indexes and not overwrite:
            return
        self.edge_indexes[key] = params
        self.init_schema()
        self.create_schema()
        index_tables = {key : self._edge_index_tables[key]}
        if initialize:
            trans = self.begin()
            for edge in self.filter_edges({}):
                self._add_to_index(edge.pk,edge.data,index_tables)
            self.commit(trans)

    def init_schema(self):

        def generate_index_tables(metadata,prefix,indexes,foreign_key_column):

            index_tables = {}
            for name,params in indexes.items():
                index_tables[name] = Table('%s_index_%s%s' % (prefix,name,self.table_postfix),metadata,
                    Column(name,params['type'],nullable = True,index = True),
                    Column('pk',self.Meta.PkType,ForeignKey(foreign_key_column),primary_key = False,index = True),
                    UniqueConstraint(name, 'pk', name='%s_%s_pk_unique_%s' % (prefix,name,self.table_postfix) )
                    )
            return index_tables

        self._metadata = MetaData()

        self._vertex = Table('vertex%s' % self.table_postfix,self._metadata,
                Column('pk',self.Meta.PkType,primary_key = True,index = True),
                Column('data',LargeBinary),
            )

        self._edge = Table('edge%s' % self.table_postfix,self._metadata,
                Column('pk',self.Meta.PkType,primary_key = True,index = True),
                Column('out_v_pk',self.Meta.PkType,ForeignKey('vertex%s.pk' % self.table_postfix),index = True),
                Column('inc_v_pk',self.Meta.PkType,ForeignKey('vertex%s.pk' % self.table_postfix),index = True),
                Column('label',VARCHAR(128),index = True),
                Column('data',LargeBinary,nullable = True),
            )

        self._vertex_index_tables = generate_index_tables(self._metadata,"vertex",self.vertex_indexes,self._vertex.c.pk)
        self._edge_index_tables = generate_index_tables(self._metadata,"edge",self.edge_indexes,self._edge.c.pk)

    def begin(self):
        return self._conn.begin()

    def commit(self,transaction):
        transaction.commit()

    def rollback(self,transaction):
        transaction.rollback()

    def close_connection(self):
        return self._conn.close()

    def create_schema(self,indexes = None):
        self.init_schema()
        self._metadata.create_all(self._engine,checkfirst = True)

    def drop_schema(self):
        self.init_schema()
        self._metadata.drop_all(self._engine,checkfirst = True)

    def __init__(self,engine,table_postfix = None,create_schema = True,use_cache = False,**kwargs):
        super(Store,self).__init__(**kwargs)
        self._engine = engine
        self._vertex_cache = {}
        self._use_cache = use_cache
        self.table_postfix = table_postfix if table_postfix is not None else self.Meta.table_postfix
        self.vertex_indexes = self.Meta.vertex_indexes.copy()
        self.edge_indexes = self.Meta.edge_indexes.copy()
        if create_schema:
            self.create_schema()
        self._conn = self._engine.connect()

    def get_edge(self,query):
        results = self.filter_edges(query)
        if not len(results):
            raise self.Meta.Edge.DoesNotExist
        elif len(results) > 1:
            raise self.Meta.Edge.MultipleEdgesReturned
        return results.pop()

    def get(self,query):
        results = self.filter(query)
        if len(results) == 0:
            raise self.Meta.Vertex.DoesNotExist
        elif len(results) > 1:
            raise self.Meta.Vertex.MultipleVerticesReturned
        return results.pop()

    def _filter(self,query,table,index_tables,deserializer):
        pk_selector = None

        native_keys = {}
        index_queries = []
        for key,value in query.items():
            if key in table.c:
                native_keys[key] = value
            elif key in index_tables:
                index_queries.append(select([index_tables[key].c.pk]).where(index_tables[key].c[key] == value))
            else:
                raise AttributeError("Query over non-indexed field: %s" % key)

        native_selector = None
        index_selector = None
        where_stmt = None

        if native_keys:
            native_selector = and_(*[table.c[key] == value for key,value in native_keys.items()])
        if index_queries:
            index_selector = table.c['pk'].in_(expression.intersect(*index_queries))

        if native_selector is not None and index_selector is not None:
            where_stmt = and_(
                    native_selector,
                    index_selector
                    )
        elif native_selector is not None:
            where_stmt = native_selector
        elif index_selector is not None:
            where_stmt = index_selector

        queryset = QuerySet(self,table,self._conn,condition = where_stmt,deserializer = deserializer)

        return queryset

    def filter(self,query):
        return self._filter(query,self._vertex,self._vertex_index_tables,self.deserialize_vertex_data)

    def filter_vertices(self,query):
        return self.filter(query)

    def filter_edges(self,query):
        return self._filter(query,self._edge,self._edge_index_tables,self.deserialize_edge_data)

    @property
    def edges(self):
        return self.filter_edges({})

    @property
    def vertices(self):
        return self.filter_vertices({})

    def reset(self):
        self.drop_schema()
        self.create_schema()

    def deserialize_edge_data(self,result):
        data = cPickle.loads(str(result['data']))
        db_data = {'label' : result['label'],
                     'out_v_pk' : result['out_v_pk'],
                     'inc_v_pk' : result['inc_v_pk'],
                     'pk' : result['pk'],
                     'data' : data}
        return self.Meta.Edge(pk = result['pk'],store = self,db_data = db_data)

    def deserialize_vertex_data(self,data):
        vertex = self.Meta.Vertex(pk = data.pk,store = self,db_data = cPickle.loads(str(data.data)))
        vertex.extra = {}
        if len(data.keys()) > 2:#we got some extra data
            for key,value in data.items():
                if key in ('pk','data'):
                    continue
                vertex.extra[key] = value
        return vertex

    def get_incoming_edges(self,pk):
        condition = self._edge.c.inc_v_pk == str(pk)
        return self._get_edges(pk,condition)

    def get_outgoing_edges(self,pk):
        condition = self._edge.c.out_v_pk == str(pk)
        return self._get_edges(pk,condition)

    def _get_edges(self,pk,condition):

        s = select([self._edge.c.pk,self._edge.c.out_v_pk,self._edge.c.inc_v_pk,
                    self._edge.c.label,self._edge.c.data]).where(condition)
        result = self._conn.execute(s)

        edges = []
        for row in result:
            edge = self.deserialize_edge_data(row)
            edges.append(edge)
        return edges

    def _get(self,pk,table,deserializer):
        condition = table.c.pk == str(pk)

        s = select([table]).where(condition)
        result = self._conn.execute(s)

        data = result.first()

        if data is None:
            raise self.Meta.Vertex.DoesNotExist

        return deserializer(data)

    def get_vertex(self,pk):
        if not self._use_cache:
            return self._get(pk,self._vertex,self.deserialize_vertex_data)
        if not pk in self._vertex_cache:
            self._vertex_cache[pk] = self._get(pk,self._vertex,self.deserialize_vertex_data)
        return self._vertex_cache[pk]

    def descendants_of(self,vertex,with_self = False):
        """
        Returns all descendants of a given vertex, i.e. all vertices that can be reached via
        outgoing edges from `vertex`. May return the vertex itself if the graph has cycles.
        """

        descendants = select([expression.cast(vertex.pk,self.Meta.PkType).label("pk")])\
                      .cte(recursive = True)

        descendants_alias = descendants.alias()
        edge_alias = self._edge.alias()
        descendants = descendants\
                      .union_all(
                        select([self._edge.c.inc_v_pk.label("pk")]).where(self._edge.c.out_v_pk == descendants_alias.c.pk))

        s = select([
                descendants.c.pk,self._vertex.c.data
            ]).select_from(descendants.join(self._vertex,self._vertex.c.pk == descendants.c.pk))

        if not with_self:
            s = s.where(self._vertex.c.pk != vertex.pk)

        queryset = QuerySet(self,self._vertex,
                            self._conn,
                            select = s,
                            deserializer = self.deserialize_vertex_data,
                            extra_fields = None)

        return queryset

    def ancestors_of(self,vertex,with_self = False):
        """
        Returns all ancestors of a given vertex, i.e. all vertices that can be reached via
        incoming edges from `vertex`. May return the vertex itself if the graph has cycles.
        """

        ancestors = select([expression.cast(vertex.pk,self.Meta.PkType).label("pk")])\
                      .cte(recursive = True)

        ancestors_alias = ancestors.alias()
        edge_alias = self._edge.alias()
        ancestors = ancestors\
                      .union_all(
                        select([self._edge.c.out_v_pk.label("pk")]).where(self._edge.c.inc_v_pk == ancestors_alias.c.pk))

        s = select([
                ancestors.c.pk,self._vertex.c.data
            ]).select_from(ancestors.join(self._vertex,self._vertex.c.pk == ancestors.c.pk))

        if not with_self:
            s = s.where(self._vertex.c.pk != vertex.pk)

        queryset = QuerySet(self,self._vertex,
                            self._conn,
                            select = s,
                            deserializer = self.deserialize_vertex_data,
                            extra_fields = None)

        return queryset

    def create_edge(self,outV,inV,label,data = None):
        serialized_data = cPickle.dumps(data)
        pk = uuid.uuid4().hex
        d = {'data' : serialized_data,
             'pk' : pk,
             'inc_v_pk' : inV.pk,
             'out_v_pk' : outV.pk,
             'label' : label,
             }
        insert = self._edge.insert().values(**d)

        trans = self.begin()
        self._conn.execute(insert)
        self.commit(trans)
        self._add_to_index(pk,data if data is not None else {},self._edge_index_tables)
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
        serialized_data = self.serialize_vertex_data(vertex.data_rw)

        try:
            trans = self.begin()
            update = self._vertex.update().values(**serialized_data)\
                                          .where(self._vertex.c.pk == str(vertex.pk))
            self._conn.execute(update)
            self._remove_vertex_from_index(vertex.pk)
            self._add_to_index(vertex.pk,vertex.data_rw,self._vertex_index_tables)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise
        return vertex

    def serialize_vertex_data(self,data):
        d = {'data' : cPickle.dumps(data)}
        if 'pk' in data:
            d['pk'] = data['pk']
        return d

    def _add_to_index(self,pk,data,index_tables):

        def add_to_index(key,table):
            if not key in data:
                return
            d = {'pk' : pk,key : data[key]}
            insert = table.insert().values(**d)
            self._conn.execute(insert)
        try:
            trans = self.begin()
            for key,table in index_tables.items():
                add_to_index(key,table)
            self.commit(trans)
        except IntegrityError:
            self.rollback(trans)
            raise

    def _remove_vertex_from_index(self,pk):
        try:
            trans = self.begin()
            for key,table in self._vertex_index_tables.items():
                delete = table.delete().where(table.c['pk'] == str(pk))
                self._conn.execute(delete)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise

    def create_vertex(self,data):
        d = self.serialize_vertex_data(data)

        if not 'pk' in data:
            d['pk'] = uuid.uuid4().hex
        else:
            d['pk'] = data['pk']

        insert = self._vertex.insert().values(**d)
        trans = self.begin()
        try:
            self._conn.execute(insert)
            self._add_to_index(d['pk'],data,self._vertex_index_tables)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise
        return self.Meta.Vertex(d['pk'],store = self,db_data = data)
