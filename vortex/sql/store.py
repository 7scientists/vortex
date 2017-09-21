# -*- coding: utf-8 -*-
from ..base import Store as BaseStore

from .edge import Edge
from .vertex import Vertex
from .queryset import QuerySet

from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import MetaData,Table,Column,ForeignKey,UniqueConstraint
from sqlalchemy.types import Integer,VARCHAR,String,Text,LargeBinary,Unicode
from sqlalchemy.sql import select,insert,update,func,and_,or_,not_,expression

import six
import ujson
import base64
import uuid
import datetime

from collections import defaultdict

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
        data = self.decode(ujson.loads(result['data'].decode('utf-8')))
        db_data = {'label' : result['label'],
                   'out_v_pk' : result['out_v_pk'],
                   'inc_v_pk' : result['inc_v_pk'],
                   'pk' : result['pk'],
                   'data' : data}
        return self.Meta.Edge(pk = result['pk'],store = self,db_data = db_data)

    def deserialize_vertex_data(self,data):
        vertex = self._create_vertex(pk = data.pk, db_data = self.decode(ujson.loads(data.data.decode('utf-8'))))
        vertex.extra = {}
        if len(data.keys()) > 2:#we got some extra data
            for key,value in data.items():
                if key in ('pk','data'):
                    continue
                vertex.extra[key] = value
        return vertex

    def get_incoming_edges(self,pk):
        condition = self._edge.c.inc_v_pk == expression.cast(pk,self.Meta.PkType)
        return self._get_edges(pk,condition)

    def get_outgoing_edges(self,pk):
        condition = self._edge.c.out_v_pk == expression.cast(pk,self.Meta.PkType)
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

    def descendants_of(self,vertex,with_self = False,limit = None,as_tree = False):
        """
        Returns all descendants of a given vertex, i.e. all vertices that can be reached via
        outgoing edges from `vertex`. May return the vertex itself if the graph has cycles.
        """

        return self.related(vertex,with_self = with_self,limit = limit,outgoing = True,as_tree = as_tree)

    def ancestors_of(self,vertex,with_self = False,limit = None,as_tree = False):
        """
        Returns all ancestors of a given vertex, i.e. all vertices that can be reached via
        incoming edges from `vertex`. May return the vertex itself if the graph has cycles.
        """

        return self.related(vertex,with_self = with_self,limit = limit,incoming = True,as_tree = as_tree)

    def related(self,vertex_or_pk,with_self = False,
                limit = 100,outgoing = False,
                incoming = False,as_tree = False):

        if isinstance(vertex_or_pk, six.string_types):
            vertex_pk = vertex_or_pk
        else:
            vertex_pk = vertex_or_pk['pk']

        if as_tree:
            initial_fields = [expression.cast(expression.literal_column("0"),Integer).label("depth"),
                              expression.cast(expression.literal_column("''"),self._edge.c.pk.type).label("edge_pk"),
                              expression.cast(expression.literal_column("''"),self._edge.c.label.type).label("edge_label"),
                              expression.cast(expression.literal_column("''"),self._edge.c.data.type).label('edge_data'),
                              expression.cast(expression.null(),self._edge.c.pk.type).label("pk_related"),
                              expression.cast(vertex_pk,self.Meta.PkType).label("pk")]
        else:
            initial_fields = [expression.cast(expression.literal_column("0"),Integer).label("depth"),
                              expression.cast(vertex_pk,self.Meta.PkType).label("pk")]

        related = select(initial_fields).cte(recursive = True)

        related_alias = related.alias()
        edge_alias = self._edge.alias()

        incoming_condition = self._edge.c.inc_v_pk == related_alias.c.pk
        outgoing_condition = self._edge.c.out_v_pk == related_alias.c.pk

        if incoming:
            condition = incoming_condition
            select_pk = self._edge.c.out_v_pk
        elif outgoing:
            condition = outgoing_condition
            select_pk = self._edge.c.inc_v_pk
        else:
            raise AttributeError("You must set either icoming or outgoing to True")

        if limit is not None:
            condition = and_(condition,related_alias.c.depth <= limit)

        if as_tree:
            select_fields = [expression.cast(related_alias.c.depth+1,Integer).label('depth'),
                             self._edge.c.pk.label('edge_pk'),
                             self._edge.c.label.label('edge_label'),
                             self._edge.c.data.label('edge_data'),
                             related_alias.c.pk.label('pk_related'),
                             select_pk.label('pk')]
        else:
            select_fields = [(related_alias.c.depth+1).label('depth'),
                             select_pk.label('pk')]

        related = related\
                      .union_all(select(select_fields).where(condition))

        if as_tree:
            output_fields = [related.c.depth,
                             related.c.edge_pk,
                             related.c.edge_label,
                             related.c.edge_data,
                             related.c.pk_related,
                             related.c.pk,
                             self._vertex.c.data,
                            ]
        else:
            output_fields = [related.c.pk.label('pk'),
                      self._vertex.c.data.label('data')]

        s = select(output_fields).select_from(related.join(self._vertex,self._vertex.c.pk == related.c.pk))

        if not with_self and not as_tree:
            s = s.where(self._vertex.c.pk != vertex_pk)

        queryset = QuerySet(self,self._vertex,
                            self._conn,
                            select = s,
                            deserializer = self.deserialize_vertex_data,
                            extra_fields = None)

        if as_tree:
            vertices = queryset.as_list()

            vertices_by_pk = {}
            related_by_pk = defaultdict(lambda: defaultdict(dict))

            #Generate a list of vertices by pk
            for v in vertices:
                vertices_by_pk[v['pk']] = v

            #Generate a list of related vertices by pk
            for v in vertices:
                related_by_pk[v.extra['pk_related']][v['pk']][v.extra['edge_pk']] = v

            for pk,vertices in related_by_pk.items():
                if pk in vertices_by_pk:
                    v = vertices_by_pk[pk]

                    edges = []
                    for vertex_dict in vertices.values():
                        for edge_pk,vv in vertex_dict.items():
                            data = {'data' : vv.extra['edge_data'],
                                    'pk' : vv.extra['edge_pk'],
                                    'label' : vv.extra['edge_label'],
                                   }
                            if incoming:
                                data['inc_v_pk'] = v['pk']
                                data['out_v_pk'] = vv['pk']
                            else:
                                data['out_v_pk'] = v['pk']
                                data['inc_v_pk'] = vv['pk']

                            edge = self.deserialize_edge_data(data)

                            if incoming:
                                edge.inV = v
                                edge.outV = vv
                            else:
                                edge.outV = v
                                edge.inV = vv

                            edges.append(edge)

                    if incoming:
                        v.inE.set_edge_list(edges)
                    else:
                        v.outE.set_edge_list(edges)

            return vertices_by_pk[vertex_pk]

        return queryset

    def update_edge(self,edge):
        if not isinstance(edge,self.Meta.Edge):
            raise TypeError("Must be an edge!")

        data = {
            'label' : edge.label,
            'inc_v_pk' : edge.inV.pk,
            'out_v_pk' : edge.outV.pk,
            'data' : self.serialize_edge_data(edge.data),
        }

        try:
            trans = self.begin()
            update = self._edge.update().values(**data)\
                                          .where(self._edge.c.pk == expression.cast(edge.pk,self.Meta.PkType))
            self._conn.execute(update)
            self._remove_edge_from_index(edge.pk)
            self._add_to_index(edge.pk,edge.data,self._edge_index_tables)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise
        return edge

    def create_edge(self,outV,inV,label,data = None):
        if not isinstance(outV,self.Meta.Vertex) or not isinstance(inV,self.Meta.Vertex):
            raise AttributeError("Invalid vertex data!")
        pk = uuid.uuid4().hex
        d = {'data' : self.serialize_edge_data(data),
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

    def delete_vertex(self,vertex):
        try:
            trans = self.begin()
            delete_edges = self._edge.delete().where(self._edge.c.inc_v_pk == vertex.pk or self._edge.c.out_v_pk == vertex.pk)
            self._conn.execute(delete_edges)
            self._remove_vertex_from_index(vertex.pk)
            delete_vertex = self._vertex.delete().where(self._vertex.c.pk == vertex.pk)
            self._conn.execute(delete_vertex)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise

    def delete_edge(self,edge):
        try:
            trans = self.begin()
            self._remove_edge_from_index(edge.pk)
            delete_edge = self._edge.delete().where(self._edge.c.pk == edge.pk)
            self._conn.execute(delete_edge)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise

    def update_vertex(self,vertex):
        if not isinstance(vertex,self.Meta.Vertex):
            raise TypeError("Must be a vertex!")
        serialized_data = self.serialize_vertex_data(vertex.data_rw)

        try:
            trans = self.begin()
            update = self._vertex.update().values(**serialized_data)\
                                          .where(self._vertex.c.pk == expression.cast(vertex.pk,self.Meta.PkType))
            self._conn.execute(update)
            self._remove_vertex_from_index(vertex.pk)
            self._add_to_index(vertex.pk,vertex.data_rw,self._vertex_index_tables)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise
        return vertex

    def serialize_vertex_data(self,data):
        d = {'data' : ujson.dumps(self.encode(data)).encode('utf-8')}
        if 'pk' in data:
            d['pk'] = data['pk']
        return d

    def serialize_edge_data(self,data):
        return ujson.dumps(self.encode(data)).encode('utf-8')

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

    def _remove_edge_from_index(self,pk):
        try:
            trans = self.begin()
            for key,table in self._edge_index_tables.items():
                delete = table.delete().where(table.c['pk'] == str(pk))
                self._conn.execute(delete)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise

    def _create_vertex(self, pk, db_data, vertex_class = None):
        if vertex_class is None:
            vertex_class = self.Meta.Vertex
        return vertex_class(pk,store = self,db_data = db_data)

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
            self._add_to_index(d['pk'], data, self._vertex_index_tables)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise
        return self._create_vertex(d['pk'], db_data=data)
