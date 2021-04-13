import dataclasses
import json
import logging
import pathlib
import random
from typing import List

from .config import BaseConfig, Union
from .pipeline import Task
from .topics import Query
from .util import ComponentFactory
from .util.file import touch_complete, DataclassJSONEncoder

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class Result:
    """Single result for a query"""
    doc_id: str
    rank: int
    score: Union[int, float]


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


class TrecResultsWriter(Task):
    """Write results to a file in TREC format"""

    def __init__(self, path):
        """
        Args:
            path (str): Path of file to write to.
        """
        super().__init__()
        self.dir = pathlib.Path(path)
        self.dir.mkdir(parents=True)
        self.path = self.dir / 'results.txt'
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
        touch_complete(self.dir)


class JsonResultsWriter(Task):
    """Write results to a json file"""

    def __init__(self, path):
        """
        Args:
            path (str): Path of file to write to.
        """
        super().__init__()
        self.dir = pathlib.Path(path)
        self.dir.mkdir(parents=True)
        self.path = self.dir / 'results.jsonl'
        self.file = open(self.path, 'w')

    def process(self, results):
        """
        Args:
            results (Results): Results for a query
        """
        self.file.write(json.dumps(results, cls=DataclassJSONEncoder) + "\n")
        return results

    def end(self):
        self.file.close()
        touch_complete(self.dir)


class JsonResultsReader:
    """Iterator over results from a jsonl file """

    def __init__(self, path):
        path = pathlib.Path(path) / 'results.jsonl'
        self.file = open(path, 'r')

    def __iter__(self):
        return self

    def __next__(self):
        line = self.file.readline()
        if not line:
            self.file.close()
            raise StopIteration
        data = json.loads(line)
        results = [Result(**result) for result in data['results']]
        return Results(Query(**data['query']), data['system'], results)


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
