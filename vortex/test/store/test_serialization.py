import gzip
import os

from . import StoreTest

class SerializationTest(StoreTest):

    test_root = os.path.join(os.path.abspath(__file__+"/../"),"data")
    tree_path = os.path.join(test_root,"2/all.pickle.gz")

    def test_raw_pickling(self):

        self.store.drop_schema()
        self.store.create_schema()

        with gzip.open(self.tree_path,"rb") as input_file:
            content = input_file.read()
            self.store.from_pickle(content)
