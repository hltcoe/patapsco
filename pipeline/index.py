import pathlib

from .config import BaseConfig
from .util import ComponentFactory


class DocumentStore:
    def __getitem__(self, doc_id):
        return "Hello, world"


class IndexerConfig(BaseConfig):
    name: str
    output: str


class IndexerFactory(ComponentFactory):
    classes = {
        'anserini': 'MockIndexer',
    }
    config_class = IndexerConfig


class Indexer:
    """Indexer interface"""
    def index(self, doc):
        """Add a document to the index

        Args:
            doc (Doc)
        """
        pass

    def close(self):
        """Close any files and release any resources"""
        pass


class MockIndexer(Indexer):
    def __init__(self, config):
        self.path = pathlib.Path(config.output) / 'index.txt'
        self.path.parent.mkdir(parents=True)
        self.file = open(self.path, 'w')

    def index(self, doc):
        self.file.write(doc.id + "\n")

    def close(self):
        self.file.close()
