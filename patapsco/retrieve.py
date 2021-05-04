import collections
import json
import logging
import pathlib
import random

from .pipeline import Task, MultiplexTask
from .results import Result, Results
from .schema import RetrieveConfig
from .util import ComponentFactory

LOGGER = logging.getLogger(__name__)


class RetrieverFactory(ComponentFactory):
    classes = {
        'bm25': 'MockRetriever',
    }
    config_class = RetrieveConfig

    @classmethod
    def create(cls, config, *args, **kwargs):
        # config.input.index.path can point to:
        #  1. a single path of a single run
        #  2. a single path of a multiplex run
        #  3. multiple paths
        if isinstance(config.input.index.path, str):
            multiplex_path = pathlib.Path(config.input.index.path) / '.multiplex'
            if not multiplex_path.exists():
                # single index
                return super().create(config, *args, **kwargs)
            else:
                # multiplex index
                with open(multiplex_path, 'r') as fp:
                    splits = json.load(fp)
                    base_path = pathlib.Path(config.input.index.path)
                    retrievers = {}
                    for split in splits:
                        copied_config = config.copy(deep=True)
                        copied_config.input.index.path = str(base_path / split)
                        retrievers[split] = super().create(copied_config, *args, **kwargs)
                    return MultiplexTask(retrievers, None, None, None)
        else:
            # multiple index paths
            paths = config.input.index.path
            retrievers = {}
            for key, path in paths.items():
                copied_config = config.copy(deep=True)
                copied_config.input.index.path = path
                retrievers[key] = super().create(copied_config, *args, **kwargs)
            return MultiplexTask(retrievers, None, None, None)


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


class MockRetriever(Task):
    """Mock retriever for testing and development"""

    def __init__(self, config):
        super().__init__()
        self.number = config.number
        self.path = pathlib.Path(config.input.index.path) / 'index.txt'
        self.doc_ids = None

    def process(self, query):
        """Retrieve a ranked list of documents

        Args:
            query (Query)

        Returns:
            Results
        """
        if not self.doc_ids:
            self._load()
        ids = random.sample(self.doc_ids, self.number)
        results = [Result(doc_id, rank, rank) for rank, doc_id in enumerate(ids)]
        return Results(query, str(self), results)

    def _load(self):
        with open(self.path, 'r') as fp:
            self.doc_ids = [line.strip() for line in fp]
        LOGGER.debug("Loaded index from %s", self.path)
