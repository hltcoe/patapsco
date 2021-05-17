from .pipeline import Task
from .schema import IndexConfig
from .util import TaskFactory
from .util.file import delete_dir, path_append


class IndexerFactory(TaskFactory):
    classes = {
        'lucene': 'LuceneIndexer',
        'mock': 'MockIndexer',
    }
    config_class = IndexConfig


class MockIndexer(Task):
    """Mock index for testing

    It writes the doc IDs to a file for later use.
    """

    def __init__(self, run_path, index_config, artifact_config):
        """
        Args:
            run_path (str): Root directory of the run.
            index_config (IndexConfig)
            artifact_config (RunnerConfig)
        """
        super().__init__(run_path, artifact_config, index_config.output)
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


class Java:
    """Wraps JVM access

    This class delays loading the JVM until needed.
    This prevents issues with multiprocessing where a child process inherits a parent's JVM.
    """
    def __init__(self):
        self.initialized = False

    def __getattr__(self, attr):
        if not self.initialized:
            self.initialize()
        return self.__dict__[attr]

    def initialize(self):
        self.initialized = True
        import pyserini.index  # required to initialize the JVM
        import jnius
        self.Document = jnius.autoclass('org.apache.lucene.document.Document')
        self.StoreEnum = jnius.autoclass('org.apache.lucene.document.Field$Store')
        self.BytesRef = jnius.autoclass('org.apache.lucene.util.BytesRef')
        self.SortedDocValuesField = jnius.autoclass('org.apache.lucene.document.SortedDocValuesField')
        self.StringField = jnius.autoclass('org.apache.lucene.document.StringField')
        self.TextField = jnius.autoclass('org.apache.lucene.document.TextField')
        self.FSDirectory = jnius.autoclass('org.apache.lucene.store.FSDirectory')
        self.Paths = jnius.autoclass('java.nio.file.Paths')
        self.WhitespaceAnalyzer = jnius.autoclass('org.apache.lucene.analysis.core.WhitespaceAnalyzer')
        self.IndexWriter = jnius.autoclass('org.apache.lucene.index.IndexWriter')
        self.IndexWriterConfig = jnius.autoclass('org.apache.lucene.index.IndexWriterConfig')


class LuceneIndexer(Task):
    """Lucene inverted index"""

    def __init__(self, run_path, index_config, artifact_config):
        """
        Args:
            run_path (str): Root directory of the run.
            index_config (IndexConfig)
            artifact_config (RunnerConfig)
        """
        super().__init__(run_path, artifact_config, index_config.output)
        self._dir = None
        self._writer = None
        self.java = Java()

    @property
    def writer(self):
        if not self._writer:
            self._dir = self.java.FSDirectory.open(self.java.Paths.get(str(self.base)))
            self._writer = self.java.IndexWriter(self._dir, self.java.IndexWriterConfig(self.java.WhitespaceAnalyzer()))
        return self._writer

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        lucene_doc = self.java.Document()
        lucene_doc.add(self.java.StringField("id", doc.id, self.java.StoreEnum.YES))
        lucene_doc.add(self.java.SortedDocValuesField("id", self.java.BytesRef(doc.id.encode())))
        lucene_doc.add(self.java.TextField("contents", doc.text, self.java.StoreEnum.NO))
        self.writer.addDocument(lucene_doc)
        return doc

    def end(self):
        super().end()
        self.close()

    def close(self):
        if self._writer:
            self._writer.close()
        if self._dir:
            self._dir.close()

    def reduce(self, dirs):
        indexes = [self.java.FSDirectory.open(self.java.Paths.get(str(item))) for item in dirs]
        self.writer.addIndexes(*indexes)
        [index.close() for index in indexes]
        [delete_dir(item) for item in dirs]
