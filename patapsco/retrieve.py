import collections
import json
import logging
import pathlib

from .error import PatapscoError
from .pipeline import Task, MultiplexTask
from .results import Result, Results
from .schema import RetrieveConfig
from .util import TaskFactory

LOGGER = logging.getLogger(__name__)


class RetrieverFactory(TaskFactory):
    classes = {
        'bm25': 'PyseriniRetriever',
    }
    config_class = RetrieveConfig

    @classmethod
    def create(cls, run_path, config, *args, **kwargs):
        """
        Args:
            run_path (str): Root directory of the run.
            config (RetrieveConfig)
        """
        # config.input.index.path can point to:
        #  1. a single path of a single run
        #  2. a single path of a multiplex run
        #  3. multiple paths
        if isinstance(config.input.index.path, str):
            multiplex_path = pathlib.Path(config.input.index.path) / '.multiplex'
            if not multiplex_path.exists():
                # single index
                return super().create(run_path, config, *args, **kwargs)
            else:
                # multiplex index
                with open(multiplex_path, 'r') as fp:
                    splits = json.load(fp)
                    base_path = pathlib.Path(config.input.index.path)
                    retrievers = {}
                    for split in splits:
                        copied_config = config.copy(deep=True)
                        copied_config.input.index.path = str(base_path / split)
                        retrievers[split] = super().create(run_path, copied_config, *args, **kwargs)
                    return MultiplexTask(retrievers)
        else:
            # multiple index paths
            paths = config.input.index.path
            retrievers = {}
            for key, path in paths.items():
                copied_config = config.copy(deep=True)
                copied_config.input.index.path = path
                retrievers[key] = super().create(run_path, copied_config, *args, **kwargs)
            return MultiplexTask(retrievers)


class Joiner(Task):
    """Join results from multiplexed retrievers"""

    def __init__(self):
        super().__init__()

    def process(self, results):
        """Join multiplexed results of a single query

        Args:
            results (MultiplexItem)

        Returns:
            Results
        """
        # get the first key/value pair and get the value (Results object)
        first_results = next(iter(results.items()))[1]
        query = first_results.query
        system = first_results.system

        # add scores, rerank, and pass as single list
        output = collections.defaultdict(int)
        for _, r in results.items():
            for result in r.results:
                output[result.doc_id] += result.score
        output = dict(sorted(output.items(), key=lambda item: item[1], reverse=True))
        output = zip(output.items(), range(len(output)))
        output = [Result(doc_id, rank, score) for (doc_id, score), rank in output]
        return Results(query, system, output)


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
        hits = self.searcher.search(query.text, k=self.number)
        LOGGER.debug(f"Retrieved {len(hits)} documents for {query.id}: {query.text}")
        results = [Result(hit.docid, rank, hit.score) for rank, hit in enumerate(hits)]
        return Results(query, str(self), results)

    def end(self):
        self.searcher.close()
