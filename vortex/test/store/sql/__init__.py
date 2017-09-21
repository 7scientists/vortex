from .. import StoreTest
from ..store_generators import sql_store_generators

class SqlStoreTest(StoreTest):

    store_generators = sql_store_generators

