import collections
import logging
import random

from .config import BaseConfig
from .pipeline import Task
from .util import ComponentFactory, trec
from .util.file import GlobFileGenerator

LOGGER = logging.getLogger(__name__)


class InputConfig(BaseConfig):
    """Qrels downstream configuration"""
    name: str = "trec"
    path: str


class ScorerConfig(BaseConfig):
    """Configuration for the scorer module"""
    metrics: list = ['map']
    input: InputConfig


class QrelsReaderFactory(ComponentFactory):
    classes = {
        'trec': 'TrecQrelsReader'
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
            config (dict)
            qrels (dict): qrels dictionary
        """
        super().__init__()
        self.config = ScorerConfig(**config)
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
        for metric in self.config.metrics:
            LOGGER.info(f"{metric} = {random.random()}")
