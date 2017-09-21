from functools import wraps

class ContainerMixin(object):

    def __init__(self,data):
        self._data = data

    def __getitem__(self,key):
        return self.data_ro[key]

    def __contains__(self,key):
        return True if key in self.data_ro else False

    def __setitem__(self,key,value):
        self.data_rw[key] = value

    def __delitem__(self,key):
        del self.data_rw[key]

    def items(self):
        return self.data_ro.items()

    def keys(self):
        return self.data_ro.keys()

    def values(self):
        return self.data_ro.values()

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    @property
    def data_ro(self):
        return self._data

    @property
    def data_rw(self):
        return self._data

    @property
    def data(self):
        return self.data_ro

    def __iter__(self):
        for key in self.data_ro:
            yield key
