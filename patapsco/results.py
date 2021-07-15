import collections
import csv
import dataclasses
import json
import logging
import pathlib
from typing import List, Union

from .pipeline import Task
from .topics import Query
from .util import DataclassJSONEncoder
from .util.file import count_lines, path_append

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
    doc_lang: str
    system: str
    results: List[Result]


class TrecResultsWriter(Task):
    """Write results to a file in TREC format

    This writes the .complete to the run directory to indicate that a job is complete.
    """

    def __init__(self, config):
        """
        Args:
            config (RunnerConfig): Config for the run.
        """
        super().__init__()
        # the base directory for results is the run_path
        self.run_path = pathlib.Path(config.run.path)  # base not set so that we don't write config/complete indicator
        self.relative_path = ''  # used by Task to provide dirs for reduce
        self.artifact_config = config
        self.filename = config.run.results
        self.path = self.run_path / self.filename
        self.file = None

    def begin(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)  # this is needed for rerank only pipelines
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
        super().end()

    def reduce(self, dirs):
        # rather than directories, we need to process files of the form [results]_part_*
        LOGGER.debug("Reducing to a single results file from %s", ', '.join(str(x) for x in dirs))
        for d in dirs:
            with open(d / self.filename) as fp:
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
        # the trec results file does not contain query language, text, or report
        self.results = iter([Results(Query(query_id, lang, '', '', None), '', system, results)
                             for query_id, results in data.items()])

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.results)

    def __str__(self):
        return self.__class__.__name__


class JsonResultsWriter(Task):
    """Write results to a json file"""

    def __init__(self, run_path, config, artifact_config):
        """
        Args:
            run_path (str): Root directory of the run.
            config (BaseConfig): Config object with output.
            artifact_config (BaseConfig): Config used to generate this artifact.
        """
        super().__init__(run_path, artifact_config, config.output)
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
        self.path = pathlib.Path(path)
        if self.path.is_dir():
            self.path = self.path / 'results.jsonl'
        self.file = open(self.path, 'r')

    def __iter__(self):
        return self

    def __next__(self):
        if self.file.closed:
            raise StopIteration
        line = self.file.readline()
        if not line:
            self.file.close()
            raise StopIteration
        data = json.loads(line)
        results = [Result(**result) for result in data['results']]
        return Results(Query(**data['query']), data['doc_lang'], data['system'], results)

    def __len__(self):
        return count_lines(str(self.path))

    def __str__(self):
        return self.__class__.__name__
