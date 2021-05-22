import logging
import pathlib

from .error import PatapscoError
from .pipeline import Task
from .results import Result, Results
from .schema import RetrieveConfig
from .util import TaskFactory

LOGGER = logging.getLogger(__name__)


class RetrieverFactory(TaskFactory):
    classes = {
        'bm25': 'PyseriniRetriever',
    }
    config_class = RetrieveConfig


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
        import pyserini.analysis
        import pyserini.search
        import jnius
        # TDOD can remove analyzer when newest version of pyserini is released
        self.WhitespaceAnalyzer = jnius.autoclass('org.apache.lucene.analysis.core.WhitespaceAnalyzer')
        self.SimpleSearcher = pyserini.search.SimpleSearcher


class PyseriniRetriever(Task):
    """Use Lucene to retrieve documents from an index"""

    def __init__(self, run_path, config):
        """
        Args:
            run_path (str or Path): Root directory of the run.
            config (RetrieveConfig)
        """
        super().__init__(run_path)
        self.number = config.number
        self.index_dir = pathlib.Path(run_path) / config.input.index.path
        self._searcher = None
        self.java = Java()
        self.lang = None  # documents language

    @property
    def searcher(self):
        if not self._searcher:
            self._searcher = self.java.SimpleSearcher(str(self.index_dir))
            self._searcher.set_analyzer(self.java.WhitespaceAnalyzer())
        return self._searcher

    def begin(self):
        try:
            lang_path = self.index_dir / ".lang"
            self.lang = lang_path.read_text()
        except IOError as e:
            raise PatapscoError(e)

    def process(self, query):
        """Retrieve a ranked list of documents

        Args:
            query (Query)

        Returns:
            Results
        """
        hits = self.searcher.search(query.query, k=self.number)
        LOGGER.debug(f"Retrieved {len(hits)} documents for {query.id}: {query.query}")
        results = [Result(hit.docid, rank, hit.score) for rank, hit in enumerate(hits)]
        return Results(query, self.lang, str(self), results)

    def end(self):
        self.searcher.close()
