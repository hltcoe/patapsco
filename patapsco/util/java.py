import jnius_config

from ..error import PatapscoError


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
            import pyserini.analysis
            import pyserini.search
            import jnius
        except Exception as e:
            msg = "Problem with Java. Likely no Java or an older JVM. Run with debug flag for more details."
            raise PatapscoError(msg) from e
        # indexing
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
        # retrieval
        self.SimpleSearcher = pyserini.search.SimpleSearcher
        self.PSQIndexSearcher = jnius.autoclass('edu.jhu.hlt.psq.search.PSQIndexSearcher')
        self.BagOfWordsQueryGenerator = jnius.autoclass('io.anserini.search.query.BagOfWordsQueryGenerator')
        self.QueryParser = jnius.autoclass('org.apache.lucene.queryparser.classic.QueryParser')
