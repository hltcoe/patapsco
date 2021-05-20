import logging

from .error import PatapscoError
from .pipeline import Task
from .schema import IndexConfig
from .util import TaskFactory
from .util.file import delete_dir

LOGGER = logging.getLogger(__name__)


class IndexerFactory(TaskFactory):
    classes = {
        'lucene': 'LuceneIndexer',
    }
    config_class = IndexConfig


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
        self.JavaException = jnius.JavaException


class LuceneIndexer(Task):
    """Lucene inverted index"""

    def __init__(self, run_path, index_config, artifact_config):
        """
        Args:
            run_path (str or Path): Root directory of the run.
            index_config (IndexConfig)
            artifact_config (RunnerConfig)
        """
        super().__init__(run_path, artifact_config, index_config.output)
        self._dir = None
        self._writer = None
        self.java = Java()
        self.lang = None

    @property
    def writer(self):
        if not self._writer:
            try:
                self._dir = self.java.FSDirectory.open(self.java.Paths.get(str(self.base)))
                self._writer = self.java.IndexWriter(self._dir, self.java.IndexWriterConfig(self.java.WhitespaceAnalyzer()))
            except self.java.JavaException as e:
                raise PatapscoError(e)
        return self._writer

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        if not self.lang:
            self.lang = doc.lang
        lucene_doc = self.java.Document()
        lucene_doc.add(self.java.StringField("id", doc.id, self.java.StoreEnum.YES))
        lucene_doc.add(self.java.SortedDocValuesField("id", self.java.BytesRef(doc.id.encode())))
        lucene_doc.add(self.java.TextField("contents", doc.text, self.java.StoreEnum.NO))
        self.writer.addDocument(lucene_doc)
        return doc

    def end(self):
        """End a job"""
        with open(self.base / '.lang', 'w') as fp:
            fp.write(self.lang)
        super().end()
        self._close()

    def _close(self):
        """Close the writer and any related resources"""
        if self._writer:
            self._writer.close()
        if self._dir:
            self._dir.close()

    def reduce(self, dirs):
        """Reduce from multiple parallel indexes to a single index"""
        LOGGER.debug("Reducing to a single lucene index from %s", ', '.join(str(x) for x in dirs))
        indexes = [self.java.FSDirectory.open(self.java.Paths.get(str(item))) for item in dirs]
        try:
            self.writer.addIndexes(*indexes)
        except self.java.JavaException as e:
            raise PatapscoError(f"Reducing parallel index failed with message: {e}")
        [index.close() for index in indexes]
        # need to record the documents language in the new index
        self.lang = (dirs[0] / ".lang").read_text()
        [delete_dir(item) for item in dirs]
