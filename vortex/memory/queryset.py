import time
import copy

class ASCENDING:
    pass

class DESCENDING:
    pass


class QuerySet(object):

    def delete(self):
        pass

    def filter(self, *args, **kwargs):
        return self.store.filter( *args, initial_keys=self.keys, **kwargs)

    def _clone(self, keys):
        return self.__class__(self.store, copy.copy(keys))

    def next(self):
        if self._i >= len(self):
            raise StopIteration
        self._i += 1
        return self[self._i - 1]

    __next__ = next

    def rewind(self):
        self._i = 0

    def sort(self, key, order=ASCENDING):
        self.keys = self.store.sort(self.keys, key, order)
        return self

    def __init__(self, store, keys):
        self.store = store
        self.keys = list(keys)
        self.objects = {}
        self.rewind()

    def __getitem__(self, i):
        if isinstance(i, slice):
            return self.__class__(self.store, self.keys[i])
        key = self.keys[i]
        if key not in self.objects:
            self.objects[key] = self.store.get_vertices_for_key(self.cls, key)
            self.objects[key]._store_key = key
        return self.objects[key]

    def __and__(self, other):
        return self._clone(set(self.keys) & set(other.keys))

    def __or__(self, other):
        return self._clone(set(self.keys) | set(other.keys))

    def __len__(self):
        return len(self.keys)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __contains__(self, obj):
        if not isinstance(obj, list) and not isinstance(obj, tuple):
            obj_list = [obj]
        else:
            obj_list = obj
        for obj in obj_list:
            try:
                storage_key = obj['pk']
            except:
                return False
            if storage_key not in self.keys:
                return False
        return True

    def __eq__(self, other):
        if isinstance(other, QuerySet): 
            if self.cls == other.cls and self.keys == other.keys:
                return True
        elif isinstance(other, list):
            if len(other) != len(self.keys):
                return False
            objs = list(self)
            if other == objs:
                return True
        return False
