import pathlib

from .config import BaseConfig
from .pipeline import Task
from .util import ComponentFactory


class IndexConfig(BaseConfig):
    """Configuration for building an index"""
    name: str
    save: str


class IndexerFactory(ComponentFactory):
    classes = {
        'anserini': 'MockIndexer',
    }
    config_class = IndexConfig


class MockIndexer(Task):
    """Mock index for testing

    It writes the doc IDs to a file for later use.
    """

    def __init__(self, config):
        """
        Args:
            config (IndexerConfig)
        """
        super().__init__()
        self.path = pathlib.Path(config.save) / 'index.txt'
        self.path.parent.mkdir(parents=True)
        self.file = open(self.path, 'w')

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        self.file.write(doc.id + "\n")
        return doc

    def end(self):
        self.file.close()
