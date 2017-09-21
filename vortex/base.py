from abc import abstractmethod
from .vertex import Vertex
from .edge import Edge

import six.moves.cPickle as pickle
import copy
import cgi

import datetime
import six
import re

class DateTimeEncoder(object):

    date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    regex = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*?Z'

    @classmethod
    def encode(cls,obj):

        if isinstance(obj, datetime.datetime):
            return obj.strftime(cls.date_format)
        return obj

    @classmethod
    def decode(cls,obj):
        if isinstance(obj, six.string_types) and re.match(cls.regex,obj):
            return datetime.datetime.strptime(obj,cls.date_format)
        return obj

class StringEncoder(object):

    @classmethod
    def encode(cls,obj):
        if isinstance(obj, six.binary_type):
            return obj
        elif isinstance(obj, six.text_type):
            return obj.encode('utf-8','ignore')
        return obj

    @classmethod
    def decode(cls,obj):
        if isinstance(obj, six.binary_type):
            return obj.decode('utf-8','ignore')
        return obj

class ComplexEncoder(object):

    @classmethod
    def encode(cls,obj):
        if isinstance(obj,complex):
            return {'_type' : 'complex','r' : obj.real,'i' : obj.imag}
        return obj

    @classmethod
    def decode(cls,obj):
        if isinstance(obj,dict) and obj.get('_type') == 'complex':
            return 1j*obj['i']+obj['r']
        return obj

class Store(object):

    """
    Manages storing, retrieving and querying abstract syntax trees from various backends.

    Abstract base class used by various other stores.
    """

    encoders = [ComplexEncoder,DateTimeEncoder,StringEncoder]

    class Meta(object):
        pass

    def __init__(self,meta = None):
        if meta is not None:
            self.Meta = meta

    @abstractmethod
    def filter(self,**kwargs):
        pass

    @property
    @abstractmethod
    def edges(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def vertices(self):
        raise NotImplementedError

    @abstractmethod
    def get_edge(self,pk):
        raise NotImplementedError

    @abstractmethod
    def get_vertex(self,pk):
        raise NotImplementedError

    @abstractmethod
    def create_vertex(self,attributes):
        raise NotImplementedError

    def delete_vertex(self,vertex):
        raise NotImplementedError

    @abstractmethod
    def get_or_create_vertex(self,attributes):
        raise NotImplementedError

    @abstractmethod
    def create_edge(self,vertex_a,vertex_b,label,data = None):
        raise NotImplementedError

    @abstractmethod
    def delete_edge(self,edge):
        raise NotImplementedError

    @abstractmethod
    def get_or_create_edge(self,vertex_a,vertex,label,data = None):
        raise NotImplementedError

    @abstractmethod
    def reset(self):
        raise NotImplementedError

    @abstractmethod
    def begin(self):
        raise NotImplementedError

    @abstractmethod
    def commit(self):
        raise NotImplementedError

    @abstractmethod
    def rollback(self):
        raise NotImplementedError

    def decode(self, data):

        d = data

        if isinstance(d, dict):
            for key,value in d.items():
                d[key] = self.decode(value)
        elif isinstance(d,(list,tuple,set)):
            d = [self.decode(e) for e in d]

        #we apply the encoders to the data (in reverse order)
        for encoder in self.encoders[::-1]:
            d = encoder.decode(d)

        return d

    def encode(self, data):

        if isinstance(data, dict):
            d = {}
            for key, value in data.items():
                d[self.encode(key)] = self.encode(value)
        elif isinstance(data, (list, tuple, set)):
            d = [self.encode(e) for e in data]
        else:
            d = data

        #we apply the encoders to the data
        for encoder in self.encoders:
            d = encoder.encode(d)

        return d

    #Common base methods for importing and exporting the graph...

    def from_dict(self,d,reset_first = True):
        if reset_first:
            self.reset()
        for vertex in d['vertices']:
            self.create_vertex(vertex)
        for edge in d['edges']:
            self.get_or_create_edge(self.get_vertex(edge['_outV']),
                          self.get_vertex(edge['_inV']),
                          label = edge['label'],
                          data = edge['data'])

    def to_dict(self):
        edges = []
        vertices =[]
        for edge in self.edges:
            e = {'label' : edge.label,
                 'data' : edge.data,
                 '_inV' : edge.inV.pk,
                 '_outV' : edge.outV.pk}
            edges.append(e)
        for vertex in self.vertices:
            n = vertex.data.copy()
            vertices.append(n)
        return {'edges' : edges,'vertices' : vertices}

    def to_pickle(self):
        return pickle.dumps(self.to_dict())

    def from_pickle(self, data):
        return self.from_dict(pickle.loads(data))

    def to_graphml(self,vertices_list = None,only_outgoing = False):
        edges = []
        vertices =[]
        vertex_keys = {}

        def serialize_vertex(vertex):
            data = ""
            for key,value in vertex.data.items():
                if value is None:
                    continue
                if key in vertex_keys and type(value) != vertex_keys[key]:
                    pass
                elif not key in vertex_keys:
                    vertex_keys[key] = type(value)
                try:
                    data+=u'<data key="%(key)s">%(value)s</data>\n' % {'key' : key,
                        'value' : cgi.escape(value
                                   if isinstance(value,(str,unicode))
                                   else unicode(value)).encode("latin1","replace")}
                except UnicodeDecodeError:
                    print("Unable to decode value %s for key %s:" % (value,key))
                    continue
                except UnicodeEncodeError:
                    print("Unable to encode value %s for key %s:" % (value,key))
                    continue
            v = '<node id="%(pk)s">%(data)s</node>' %{
                'pk' : vertex.pk,
                'data' : data
            }
            return v

        def serialize_edge(edge):
            data = '<data key="label">%s</data>' % edge.label
            e = '<edge id="%(pk)s" directed="true" source="%(outV)s" target="%(inV)s">%(data)s</edge>' % {
                'pk' : edge.pk,
                'inV' : edge.inV.pk,
                'outV' : edge.outV.pk,
                'data' : data
            }
            return e
        serialized_vertices = {}
        serialized_edges = {}
        if vertices_list:#we only serialize the vertices and associated edges in vertices_list
            vertices_to_serialize = vertices_list[:]
            edges_to_serialize = []
            while vertices_to_serialize:
                vertex = vertices_to_serialize.pop()
                vertices.append(serialize_vertex(vertex))
                serialized_vertices[vertex['pk']] = True
                for edge in vertex.outE:
                    if edge.pk in serialized_edges:
                        continue
                    serialized_edges[edge.pk] = True
                    edges.append(serialize_edge(edge))
                    if not edge.inV['pk'] in serialized_vertices:
                        vertices_to_serialize.append(edge.inV)
                if not only_outgoing:
                    for edge in vertex.inE:
                        if edge.pk in serialized_edges:
                            continue
                        serialized_edges[edge.pk] = True
                        edges.append(serialize_edge(edge))
                        if not edge.outV['pk'] in serialized_vertices:
                            vertices_to_serialize.append(edge.outV)
        else:#we serialize EVERYTHING
            for edge in self.edges:
                e = serialize_edge(edge)
                edges.append(e)
            for vertex in self.vertices:
                v = serialize_vertex(vertex)
                vertices.append(v)

        def type_str(t):
            if t == list:
                return "list"
            elif t == str:
                return "string"
            elif t == float:
                return "float"
            elif t == int:
                return "int"
            return "string"
        vertex_keys_str = "\n".join(['<key id="%(key)s" for="node" attr.name="%(key)s" attr.type="%(t)s"/>' % {'key' : key,'t' : type_str(t)} for key,t in vertex_keys.items()])
        return """<?xml version="1.0" encoding="utf-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
    <key id="label" for="edge" attr.name="label" attr.type="string"/>
    %(vertex_keys_str)s
    <graph id="G" edgedefault="directed">
        %(vertices)s
        %(edges)s
    </graph>
</graphml>""" % {'vertices' : "\n".join(vertices),'edges' : "\n".join(edges),'vertex_keys_str' : vertex_keys_str}
