import pyserini.index
import jnius

from .pipeline import Task
from .schema import IndexConfig
from .util import ComponentFactory
from .util.file import path_append


class IndexerFactory(ComponentFactory):
    classes = {
        'lucene': 'LuceneIndexer',
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
            index_config (IndexConfig)
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


JDocument = jnius.autoclass('org.apache.lucene.document.Document')
JStoreEnum = jnius.autoclass('org.apache.lucene.document.Field$Store')
JBytesRef = jnius.autoclass('org.apache.lucene.util.BytesRef')
JSortedDocValuesField = jnius.autoclass('org.apache.lucene.document.SortedDocValuesField')
JStringField = jnius.autoclass('org.apache.lucene.document.StringField')
JTextField = jnius.autoclass('org.apache.lucene.document.TextField')
JFSDirectory = jnius.autoclass('org.apache.lucene.store.FSDirectory')
JPaths = jnius.autoclass('java.nio.file.Paths')
JWhitespaceAnalyzer = jnius.autoclass('org.apache.lucene.analysis.core.WhitespaceAnalyzer')
JIndexWriter = jnius.autoclass('org.apache.lucene.index.IndexWriter')
JIndexWriterConfig = jnius.autoclass('org.apache.lucene.index.IndexWriterConfig')


class LuceneIndexer(Task):
    """Lucene inverted index"""

    def __init__(self, index_config, artifact_config):
        """
        Args:
            index_config (IndexConfig)
            artifact_config (RunnerConfig)
        """
        super().__init__(artifact_config, index_config.output.path)
        self.dir = JFSDirectory.open(JPaths.get(str(index_config.output.path)))
        self.writer = JIndexWriter(self.dir, JIndexWriterConfig(JWhitespaceAnalyzer()))

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        lucene_doc = JDocument()
        lucene_doc.add(JStringField("id", doc.id, JStoreEnum.YES))
        lucene_doc.add(JSortedDocValuesField("id", JBytesRef(doc.id.encode())))
        lucene_doc.add(JTextField("contents", doc.text, JStoreEnum.NO))
        self.writer.addDocument(lucene_doc)
        return doc

    def end(self):
        super().end()
        self.writer.close()
        self.dir.close()

    # TODO need to implement combining indexes
    # def reduce(self, dirs):
