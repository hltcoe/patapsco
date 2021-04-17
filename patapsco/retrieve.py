import logging
import pathlib
import random

from .config import BaseConfig, Optional, PathConfig, Union
from .pipeline import Task
from .results import Result, Results
from .util import ComponentFactory

LOGGER = logging.getLogger(__name__)


class RetrieveInputConfig(BaseConfig):
    """Configuration of optional retrieval inputs"""
    index: PathConfig
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
        return Results(query, 'MockRetriever', results)

    def _load(self):
        with open(self.path, 'r') as fp:
            self.doc_ids = [line.strip() for line in fp]
        LOGGER.debug("Loaded index from %s", self.path)
