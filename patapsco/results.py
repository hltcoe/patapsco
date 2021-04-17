import dataclasses
import json
import pathlib
from typing import List, Union

from .pipeline import Task
from .topics import Query
from .util import DataclassJSONEncoder
from .util.file import touch_complete


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
