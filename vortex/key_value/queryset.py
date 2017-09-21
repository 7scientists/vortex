import time
import copy

from sqlalchemy.sql import select,func,expression

from functools import wraps

class ASCENDING:
    pass

class DESCENDING:
    pass

class QuerySet(object):

    def __init__(self, store, table, connection, deserializer,
                 condition = None,select = None,extra_fields = None,intersects = None,
                 ):
        self.store = store
        self.condition = condition
        self.select = select
        self.deserializer = deserializer
        self.connection = connection
        self.table = table
        self.extra_fields = extra_fields if extra_fields is not None else []
        self.count = None
        self.result = None
        self.intersects = intersects
        self.objects = None
        self.pop_objects = None

    def __iter__(self):
        if self.objects is None:
            self.get_objects()
        for obj in self.objects:
            yield self.deserializer(obj)
        raise StopIteration

    def get_objects(self):
        s = self.get_select()
        self.objects = self.connection.execute(s).fetchall()
        self.pop_objects = self.objects[:]

    def __getitem__(self,i):
        if self.objects is None:
            self.get_objects()
        return self.deserializer(self.objects[i])

    def pop(self,i = 0):
        if self.objects is None:
            self.get_objects()
        if self.pop_objects:
            return self.deserializer(self.pop_objects.pop())
        raise IndexError("No more results!")

    def get_select(self):
        if self.condition is not None:
            s = select([self.table]).where(self.condition)
        elif self.select is not None:
            s = self.select
        else:
            s = select([self.table])
        return s

    def intersect(self,queryset):
        s1 = self.get_select()
        s2 = queryset.get_select()
        if self.intersects:
            intersects = self.intersects[:]
            intersects.append(s2)
        else:
            intersects = [s1,s2]

        i = expression.intersect(*intersects)

        qs = QuerySet(self.store,self.table,self.connection,self.deserializer,select = i,intersects = intersects)
        return qs

    def __len__(self):
        if self.count is None:
            s = self.get_select()
            count_select = select([func.count(s.alias("count").c.pk)])
            result = self.connection.execute(count_select)
            self.count = result.first()[0]
            result.close()
        return self.count
        

