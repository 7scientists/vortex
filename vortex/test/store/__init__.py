from functools import wraps
from unittest import TestCase

import six

from .store_generators import all_store_generators

class TestGenerator(type):

    def __new__(cls,name,bases,attrs):

        def backend_wrapper(f):

            @wraps(f)
            def wrapper(self ,*args, **kwargs):

                for StoreGeneratorCls in self.store_generators:
                    print("Testing with %s" % StoreGeneratorCls.__name__)
                    generator = StoreGeneratorCls(self)
                    self.store = generator.store
                    try:
                        f(self,*args,**kwargs)
                    finally:
                        generator.cleanup()

            return wrapper

        for name in attrs:
            if name.startswith("test_") and callable(attrs[name]):
                attrs[name] = backend_wrapper(attrs[name])

        return super(TestGenerator,cls).__new__(cls,name,bases,attrs)

@six.add_metaclass(TestGenerator)
class StoreTest(TestCase):

    store_generators = all_store_generators
