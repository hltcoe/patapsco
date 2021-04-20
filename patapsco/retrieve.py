import itertools
import logging
import pathlib
import random

from .config import BaseConfig, Optional, PathConfig, Union
from .pipeline import Task, MultiplexTask
from .results import Result, Results
from .util import ComponentFactory

LOGGER = logging.getLogger(__name__)


class RetrieveIndexPathConfig(BaseConfig):
    path: dict  # index name: index path


class RetrieveInputConfig(BaseConfig):
    """Configuration of optional retrieval inputs"""
    index: Union[PathConfig, RetrieveIndexPathConfig]
    queries: Optional[PathConfig]


class RetrieveConfig(BaseConfig):
    """Configuration for retrieval"""
    name: str
    number: int = 1000
    input: RetrieveInputConfig
    output: Union[bool, PathConfig]


class RetrieverFactory(ComponentFactory):
    classes = {
        'bm25': 'MockRetriever',
    }
    config_class = RetrieveConfig

    @classmethod
    def create(cls, config, *args, **kwargs):
        if isinstance(config.input.index.path, str):
            return super().create(config, *args, **kwargs)
        else:
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
        return Results(query, self.name, results)

    def _load(self):
        with open(self.path, 'r') as fp:
            self.doc_ids = [line.strip() for line in fp]
        LOGGER.debug("Loaded index from %s", self.path)
