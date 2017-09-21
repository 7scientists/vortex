import uuid
from collections import defaultdict
from vortex.utils.serializable import SerializableMixin
from vortex.utils.container import ContainerMixin

class Edge(SerializableMixin):

    def __init__(self,outV,inV,label,data = None):
        super(Edge,self).__init__()
        self.pk = uuid.uuid4().hex
        self._outV = outV
        self._inV = inV
        self._label = label
        self._data = data
        outV.outE.append(self)
        inV.inE.append(self)

    def __repr__(self):
        return self.__class__.__name__+"(label = \""+ \
               str(self._label)+"\", outV = "+repr(self._outV)+ \
               ", inV = "+repr(self._inV)+", data = "+repr(self.data)+")"

    @property
    def inV(self):
        return self._inV

    @property
    def outV(self):
        return self._outV

    @property
    def label(self):
        return self._label

    @property
    def data(self):
        return self._data

    def as_dict(self):
        d = super(Edge,self).as_dict()
        d['label'] = self.label
        d['data'] = self.data
        return d
