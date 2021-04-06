import pathlib

from .config import BaseConfig
from .pipeline import Module
from .util import ComponentFactory


class IndexerConfig(BaseConfig):
    """Configuration for building an index"""
    name: str
    save: str


class IndexerFactory(ComponentFactory):
    classes = {
        'anserini': 'MockIndexer',
    }
    config_class = IndexerConfig


class MockIndexer(Module):
    """Mock index for testing

    It writes the doc IDs to a file for later use.
    """

    def __init__(self, config, input):
        """
        Args:
            config (IndexerConfig)
            input (iterator): Iterator over Document objects
        """
        super().__init__(input)
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
        super().end()
        self.file.close()
