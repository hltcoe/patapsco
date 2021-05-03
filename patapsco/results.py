import collections
import csv
import dataclasses
import json
import pathlib
from typing import List, Union

from .config import ConfigService
from .pipeline import Task
from .topics import Query
from .util import DataclassJSONEncoder
from .util.file import path_append, touch_complete


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

    def __init__(self, config, artifact_config):
        """
        Args:
            config (BaseConfig): Config object with output.path.
            artifact_config (BaseConfig): Config used to create this artifact.
        """
        super().__init__(artifact_config, config.output.path)
        self.path = self.base / 'results.txt'
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
        super().end()
        self.file.close()

    def reduce(self, dirs):
        for base in dirs:
            path = path_append(base, 'results.txt')
            with open(path) as fp:
                for line in fp:
                    self.file.write(line)


class TrecResultsReader:
    """Iterator over results from a trec format output file """

    def __init__(self, path, sep=' ', lang=None):
        """
        Args:
            path (str): Path to the results file.
            sep (str): Optional separator of columns.
            lang (str): Optional language of the queries.
        """
        system = None
        data = collections.defaultdict(list)
        with open(path, 'r') as fp:
            reader = csv.reader(fp, delimiter=sep)
            for row in reader:
                system = row[5]
                data[row[0]].append(Result(row[2], int(row[3]), float(row[4])))
        # the trec results file does not contain language or the query text
        self.results = iter([Results(Query(query_id, lang, None), system, results)
                             for query_id, results in data.items()])

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.results)

    def __str__(self):
        return self.__class__.__name__


class JsonResultsWriter(Task):
    """Write results to a json file"""

    def __init__(self, config, artifact_config):
        """
        Args:
            config (OutputConfig): Config object with output.path.
            artifact_config (BaseConfig): Config used to generate this artifact.
        """
        super().__init__(artifact_config, config.output.path)
        self.path = self.base / 'results.jsonl'
        self.file = open(self.path, 'w')

    def process(self, results):
        """
        Args:
            results (Results): Results for a query
        """
        self.file.write(json.dumps(results, cls=DataclassJSONEncoder) + "\n")
        return results

    def end(self):
        super().end()
        self.file.close()

    def reduce(self, dirs):
        for base in dirs:
            path = path_append(base, 'results.jsonl')
            with open(path) as fp:
                for line in fp:
                    self.file.write(line)


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

    def __str__(self):
        return self.__class__.__name__
