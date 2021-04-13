import collections
import dataclasses
import logging
import pathlib
import random
from typing import List

from .config import BaseConfig, Union
from .pipeline import Task
from .topics import Query
from .util import ComponentFactory

LOGGER = logging.getLogger(__name__)

Result = collections.namedtuple('Result', ('doc_id', 'rank', 'score'))


@dataclasses.dataclass
class Results:
    """Results for a query"""
    query: Query
    system: str
    results: List[Result]


class RetrieveInputConfig(BaseConfig):
    """Configuration of retrieval index location"""
    path: str


class RetrieveConfig(BaseConfig):
    """Configuration for retrieval"""
    name: str
    number: int = 1000
    save: Union[bool, str]
    input: RetrieveInputConfig


class RetrieverFactory(ComponentFactory):
    classes = {
        'bm25': 'MockRetriever',
    }
    config_class = RetrieveConfig


class ResultsWriter(Task):
    """Write results to a file"""

    def __init__(self, path):
        """
        Args:
            path (str): Path of file to write to.
        """
        super().__init__()
        directory = pathlib.Path(path)
        directory.mkdir(parents=True)
        self.path = directory / 'results.txt'
        self.file = open(self.path, 'w')

    def process(self, results):
        """
        Args:
            results (Results): Results for a query
        """
        for result in results.results:
            self.file.write(f"{results.query.id} Q0 {result.doc_id} {result.rank} {result.score} {results.system}\n")
        return results

    def end(self):
        self.file.close()


class MockRetriever(Task):
    """Mock retriever for testing and development"""

    def __init__(self, config):
        super().__init__()
        self.number = config.number
        self.path = pathlib.Path(config.input.path) / 'index.txt'
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
