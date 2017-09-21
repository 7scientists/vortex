import os 

from sqlalchemy import create_engine

from vortex.sql import Store
from sqlalchemy.types import String,VARCHAR
from sqlalchemy.schema import MetaData

class NodeStore(Store):

    class Meta(Store.Meta):
        vertex_indexes = {'node_type' : {'type' : VARCHAR(64)}}


class StoreGenerator(object):

    def __init__(self,test_class):
        self.test_class = test_class
        self.engine = self.get_engine()
        self.store = self.get_store()

    def get_store(self):
        raise NotImplementedError

    def get_engine(self):
        raise NotImplementedError

    def cleanup(self):
        pass

    def __del__(self):
        pass

class SqlMemoryStoreGenerator(StoreGenerator):

    def get_engine(self):
        return create_engine('sqlite:///:memory:', echo=False)

    def get_store(self):
        if hasattr(self.test_class,'Meta'):
            return NodeStore(self.engine,meta = self.test_class.Meta)
        return NodeStore(self.engine)

sql_store_generators = [SqlMemoryStoreGenerator]
all_store_generators = [SqlMemoryStoreGenerator]

if 'vortex_MSSQL_STORE' in os.environ:

    class MSSqlStoreGenerator(SqlMemoryStoreGenerator):

        def get_engine(self):
            import urllib
            import pyodbc

            def connection_string():
                quoted = urllib.quote_plus(os.environ['vortex_MSSQL_STORE'])
                return 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)

            engine = create_engine(connection_string(),
                echo=False,
                deprecate_large_types = True)
            self.clear_schema(engine)
            return engine

        def clear_schema(self,engine):

            metadata = MetaData()
            metadata.reflect(bind = engine)
            metadata.drop_all(engine,checkfirst = True)

        def get_store(self):
            store = super(MSSqlStoreGenerator,self).get_store()
            return store

        def cleanup(self):
            self.store.close_connection()

            self.clear_schema(self.engine)

    sql_store_generators.append(MSSqlStoreGenerator)
    all_store_generators.append(MSSqlStoreGenerator)


if 'vortex_PSQL_STORE' in os.environ:

    class PostgresSqlStoreGenerator(SqlMemoryStoreGenerator):

        def get_engine(self):
            engine = create_engine('postgres://%s' % os.environ['vortex_PSQL_STORE'], echo=False)
            self.clear_schema(engine)
            return engine

        def clear_schema(self,engine):

            metadata = MetaData()
            metadata.reflect(bind = engine)
            metadata.drop_all(engine,checkfirst = True)

        def get_store(self):
            store = super(PostgresSqlStoreGenerator,self).get_store()
            return store

        def cleanup(self):
            self.store.close_connection()

            self.clear_schema(self.engine)

    sql_store_generators.append(PostgresSqlStoreGenerator)
    all_store_generators.append(PostgresSqlStoreGenerator)

if 'vortex_ORIENTDB_STORE' in os.environ:

    from vortex.orientdb import Store as OrientDBStore
    import pyorient

    class OrientDBStoreGenerator(SqlMemoryStoreGenerator):

        def get_client(self):
            self.client = pyorient.OrientDB(os.environ["vortex_ORIENTDB_STORE"],os.environ.get('vortex_ORIENTDB_PORT',2424))
            session_id = self.client.connect(os.environ.get("vortex_ORIENTDB_USER",'guest'),os.environ.get("vortex_ORIENTDB_PASSWORD",'guest'))
            return self.client

        def clear_schema(self,client):
            pass

        def get_store(self):
            store = OrientDBStore(self.get_client())
            return store

        def cleanup(self):
            pass

    all_store_generators.append(OrientDBStoreGenerator)
