import collections
import logging
import pytrec_eval
import random

from .config import BaseConfig
from .pipeline import Task
from .util import ComponentFactory, trec
from .util.file import GlobFileGenerator

LOGGER = logging.getLogger(__name__)


class InputConfig(BaseConfig):
    """Qrels downstream configuration"""
    format: str = "trec"
    path: str


class ScoreConfig(BaseConfig):
    """Configuration for the scorer module"""
    metrics: list = ['map']
    input: InputConfig


class QrelsReaderFactory(ComponentFactory):
    classes = {
        'trec': 'TrecQrelsReader',
        'msmarco': 'TrecQrelsReader',
    }
    config_class = InputConfig


class TrecQrelsReader:
    """Read TREC qrels files"""

    def __init__(self, config):
        self.path = config.path
        self.qrels_iter = GlobFileGenerator(config.path, trec.parse_qrels)

    def read(self):
        """
        Returns:
            dictionary of query_id -> {doc_id: relevance}
        """
        data = {}
        for qrels in self.qrels_iter:
            data = {**data, **qrels}
        return data


class Scorer(Task):
    """Scorer module"""

    def __init__(self, config, qrels):
        """
        Args:
            config (ScoreConfig)
            qrels (dict): qrels dictionary
        """
        super().__init__()
        self.config = config
        self.config.metrics = [m.replace('@', '_').capitalize()
                               if m[:2] == 'p@' else m.replace('@', '_')
                               for m in self.config.metrics]
        self.qrels = qrels
        self.run = collections.defaultdict(dict)

    def process(self, results):
        """ Accumulate the results and calculate scores at end

        Args:
            results (Results): Results for a query

        Return:
            Results
        """
        for result in results.results:
            self.run[results.query.id][result.doc_id] = result.score
        return results

    def end(self):
        measures = {s for s in self.config.metrics}
        evaluator = pytrec_eval.RelevanceEvaluator(self.qrels, measures)
        res = evaluator.evaluate(self.run)
        for q, results_dict in res.items():
            LOGGER.info(f"{q} = {results_dict}")
