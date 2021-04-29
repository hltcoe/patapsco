from .pipeline import Task
from .schema import IndexConfig
from .util import ComponentFactory
from .util.file import path_append


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
        super().__init__(artifact_config, index_config.output.path)
        path = self.base / 'index.txt'
        self.file = open(path, 'w')

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

    def reduce(self, dirs):
        for base in dirs:
            path = path_append(base, 'index.txt')
            with open(path) as fp:
                for line in fp:
                    self.file.write(line)
