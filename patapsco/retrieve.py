import itertools
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
        """Join multiplexed results

        Args:
            results (MultiplexItem)

        Returns:
            Results
        """
        # TODO how to combine - probably union - but how to treat scores and ranks?
        # TODO which query to pass
        # get the first key/value pair and get the value
        first_results = next(iter(results.items()))[1]
        query = first_results.query
        system = first_results.system
        results = [res.results for key, res in results.items()]
        results = list(itertools.chain(*results))
        return Results(query, system, results)


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
