import logging
import pathlib

import jnius_config

from .error import PatapscoError
from .pipeline import Task
from .schema import IndexConfig
from .util import TaskFactory

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
        if not jnius_config.vm_running:
            jnius_config.add_options('-Xmx1024m')  # restrict Java's heap size as requested by HLTCOE IT staff
        try:
            import pyserini.index  # required to initialize the JVM
            import jnius
        except Exception as e:
            msg = "Problem with Java. Likely no Java or an older JVM. Run with debug flag for more details."
            raise PatapscoError(msg) from e
        self.String = jnius.autoclass('java.lang.String')
        self.CharSequence = jnius.autoclass('java.lang.CharSequence')
        self.Document = jnius.autoclass('org.apache.lucene.document.Document')
        self.StoreEnum = jnius.autoclass('org.apache.lucene.document.Field$Store')
        self.SortedDocValuesField = jnius.autoclass('org.apache.lucene.document.SortedDocValuesField')
        self.Field = jnius.autoclass('org.apache.lucene.document.Field')
        self.FieldType = jnius.autoclass('org.apache.lucene.document.FieldType')
        self.StringField = jnius.autoclass('org.apache.lucene.document.StringField')
        self.TextField = jnius.autoclass('org.apache.lucene.document.TextField')
        self.Paths = jnius.autoclass('java.nio.file.Paths')
        self.WhitespaceAnalyzer = jnius.autoclass('org.apache.lucene.analysis.core.WhitespaceAnalyzer')
        self.IndexOptions = jnius.autoclass('org.apache.lucene.index.IndexOptions')
        self.IndexWriter = jnius.autoclass('org.apache.lucene.index.IndexWriter')
        self.IndexWriterConfig = jnius.autoclass('org.apache.lucene.index.IndexWriterConfig')
        self.FSDirectory = jnius.autoclass('org.apache.lucene.store.FSDirectory')
        self.BytesRef = jnius.autoclass('org.apache.lucene.util.BytesRef')
        self.JavaException = jnius.JavaException
        self.cast = jnius.cast


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
        self.field_type = None

    @property
    def writer(self):
        if not self._writer:
            try:
                self._dir = self.java.FSDirectory.open(self.java.Paths.get(str(self.base)))
                self._writer = self.java.IndexWriter(self._dir, self.java.IndexWriterConfig(self.java.WhitespaceAnalyzer()))
            except self.java.JavaException as e:
                raise PatapscoError(e)
        return self._writer

    def _create_field_type(self):
        self.field_type = self.java.FieldType()
        self.field_type.setStored(False)
        self.field_type.setTokenized(True)
        self.field_type.setStoreTermVectors(True)
        self.field_type.setIndexOptions(self.java.IndexOptions.DOCS_AND_FREQS)
        self.field_type.freeze()

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        if not self.lang:
            self.lang = doc.lang
        if not self.field_type:
            self._create_field_type()

        lucene_doc = self.java.Document()
        lucene_doc.add(self.java.StringField("id", doc.id, self.java.StoreEnum.YES))
        lucene_doc.add(self.java.SortedDocValuesField("id", self.java.BytesRef(doc.id.encode())))
        text = self.java.cast(self.java.CharSequence, self.java.String(doc.text.encode('utf-8')))  # jnius requires this cast
        lucene_doc.add(self.java.Field("contents", text, self.field_type))
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
        self.lang = (pathlib.Path(dirs[0]) / ".lang").read_text()
