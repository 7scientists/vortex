from vortex.utils.serializable import SerializableMixin
from abc import abstractmethod

class Edge(SerializableMixin):

    class DoesNotExist(BaseException):
        pass

    class MultipleEdgesReturned(BaseException):
        pass

    def __repr__(self):
        return self.__class__.__name__+"(label = \""+ \
               str(self.label)+"\", outV = "+repr(self.outV)+ \
               ", inV = "+repr(self.inV)+", data = "+repr(self.data)+")"

    @property
    @abstractmethod
    def inV(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def outV(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def label(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def data(self):
        raise NotImplementedError

    def as_dict(self):
        d = super(Edge,self).as_dict()
        d['label'] = self.label
        d['data'] = self.data
        return d
