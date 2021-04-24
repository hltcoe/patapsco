import pathlib

from .config import ConfigService
from .pipeline import Task
from .schema import IndexConfig
from .util import ComponentFactory
from .util.file import touch_complete


class IndexerFactory(ComponentFactory):
    classes = {
        'anserini': 'MockIndexer',
        'mock': 'MockIndexer',
    }
    config_class = IndexConfig


class MockIndexer(Task):
    """Mock index for testing

    It writes the doc IDs to a file for later use.
    """

    def __init__(self, index_config, artifact_config):
        """
        Args:
            index_config (IndexerConfig)
            artifact_config (RunnerConfig)
        """
        super().__init__()
        self.dir = pathlib.Path(index_config.output.path)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / 'index.txt'
        self.file = open(self.path, 'w')
        self.artifact_config = artifact_config
        self.config_path = self.dir / 'config.yml'

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
        ConfigService.write_config_file(self.config_path, self.artifact_config)
        touch_complete(self.dir)
