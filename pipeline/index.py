import pathlib

from .config import BaseConfig
from .error import ConfigError


class Indexer:
    """Indexer interface"""
    def index(self, doc):
        """Tokenize text

        Args:
            doc (Doc)
        """
        pass

    def close(self):
        """Close any files and release any resources"""
        pass


class DumbIndexer(Indexer):
    def __init__(self, config):
        self.path = pathlib.Path (config.output) / 'index.txt'
        self.path.parent.mkdir(parents=True)
        self.file = open(self.path, 'w')

    def index(self, doc):
        self.file.write(doc.id + "\n")

    def close(self):
        self.file.close()


class IndexerConfig(BaseConfig):
    name: str
    output: str


class IndexerFactory:
    classes = {
        'anserini': DumbIndexer,
    }

    @classmethod
    def create(cls, config):
        config = IndexerConfig(**config)
        if config.name not in cls.classes:
            raise ConfigError(f"Unknown indexer: {config.name}")
        return cls.classes[config.name](config)
