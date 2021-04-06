import logging
import pathlib
import random

from .config import BaseConfig
from .core import Result
from .util import ComponentFactory

LOGGER = logging.getLogger(__name__)


class RetrieveConfig(BaseConfig):
    name: str
    number: int = 1000
    input: str
    output: str


class RetrieverFactory(ComponentFactory):
    classes = {
        'bm25': 'MockRetriever',
    }
    config_class = RetrieveConfig


class Retriever:
    """Retrieval interface"""

    def retrieve(self, topic_id, query):
        """Retrieve a ranked list of documents

        Args:
            query_id (str)
            query (str)

        Returns:
            list of Result
        """
        pass

    def close(self):
        """Close any files and release any resources"""
        pass


class MockRetriever(Retriever):
    def __init__(self, config):
        self.number = config.number
        self.path = pathlib.Path(config.input) / 'index.txt'
        self.doc_ids = None

    def retrieve(self, query_id, query):
        if not self.doc_ids:
            self._load()
        ids = random.sample(self.doc_ids, self.number)
        return [Result(query_id, doc_id, rank, rank, 'MockRetriever') for rank, doc_id in enumerate(ids)]

    def _load(self):
        with open(self.path, 'r') as fp:
            self.doc_ids = [line.strip() for line in fp]
        LOGGER.debug("Loaded index from %s", self.path)
