import abc
import logging
import pathlib

import jnius_config

from .error import PatapscoError
from .pipeline import Task
from .results import Result, Results
from .schema import RetrieveConfig
from .util import TaskFactory

LOGGER = logging.getLogger(__name__)


class RetrieverFactory(TaskFactory):
    classes = {
        'bm25': 'BM25Retriever',
        'qld': 'QLDRetriever',
        'rm3': 'RM3Retriever',
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
        if not jnius_config.vm_running:
            jnius_config.add_options('-Xmx500m')  # restrict Java to 500 MB which is enough for Anserini/Lucene
        try:
            import pyserini.analysis
            import pyserini.search
            import jnius
        except Exception as e:
            msg = "Problem with Java. Likely no Java or an older JVM. Run with debug flag for more details."
            raise PatapscoError(msg) from e
        # TODO can remove analyzer when newest version of pyserini is released
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
        self.config = config
        self.number = self.config.number
        self.index_dir = pathlib.Path(run_path) / self.config.input.index.path
        self._searcher = None
        self.java = Java()
        self.lang = None  # documents language

    @property
    @abc.abstractmethod
    def searcher(self):
        raise NotImplementedError

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


class BM25Retriever(PyseriniRetriever):
    """Use Lucene to retrieve documents from an index"""

    def __init__(self, run_path, config):
        super().__init__(run_path, config)

    @property
    def searcher(self):
        if not self._searcher:
            k1 = self.config.input.k1
            b = self.config.input.b
            self._searcher = self.java.SimpleSearcher(str(self.index_dir))
            self._searcher.set_analyzer(self.java.WhitespaceAnalyzer())
            self._searcher.set_bm25(k1, b)
            LOGGER.info(f'Using BM25 parameters k1={k1} and b={b}')
        return self._searcher


class QLDRetriever(PyseriniRetriever):
    """Use Query Likelihood to retrieve documents from an index"""

    def __init__(self, run_path, config):
        super().__init__(run_path, config)

    @property
    def searcher(self):
        if not self._searcher:
            mu = self.config.input.mu
            self._searcher = self.java.SimpleSearcher(str(self.index_dir))
            self._searcher.set_qld(mu)
            LOGGER.info(f'Using QLD parameter mu={mu}')
        return self._searcher


class RM3Retriever(PyseriniRetriever):
    """Use RM3 query expansion with Lucene to retrieve documents from an index"""

    def __init__(self, run_path, config):
        super().__init__(run_path, config)

    @property
    def searcher(self):
        if not self._searcher:
            k1 = self.config.input.k1
            b = self.config.input.b
            fb_terms = self.config.input.fb_terms
            fb_docs = self.config.input.fb_docs
            original_query_weight = self.config.input.original_query_weight
            self._searcher = self.java.SimpleSearcher(str(self.index_dir))
            self._searcher.set_analyzer(self.java.WhitespaceAnalyzer())
            self._searcher.set_bm25(k1, b)
            self._searcher.set_rm3(fb_terms, fb_docs, original_query_weight)
        return self._searcher
